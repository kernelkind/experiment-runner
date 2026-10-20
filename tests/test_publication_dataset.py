"""Shared publication values preserve independent trials and exact source bytes."""
import hashlib
import json
from pathlib import Path
import tempfile
import tracemalloc
import unittest


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode()


class PublicationDatasetTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root/'source'
        self.source.mkdir()
        self.output = self.root/'dataset'
        self.rules = [dict(format='trial-v1', file='results.jsonl', path=[name])
                      for name in ('input', 'evidence')]
        self.value = dict(text='A shared scientific input. ' * 30, masks=[[0, 3]])

    def publication(self, name, *, status='completed', rows=None):
        directory = self.source/name
        directory.mkdir(parents=True)
        if rows is None:
            rows = [dict(trial=name, status=status, input=self.value, evidence=self.value)]
        raw = b''.join(canonical(row)+b'\n' for row in rows)
        (directory/'results.jsonl').write_bytes(raw)
        metadata = dict(format='trial-v1', schema_version=1, source_id=name,
            status=status, files={'results.jsonl':dict(sha256=hashlib.sha256(raw).hexdigest(),
                                                    bytes=len(raw), records=len(rows))})
        meta_raw = json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2).encode()+b'\n'
        (directory/'experiment.json').write_bytes(meta_raw)
        return hashlib.sha256(meta_raw).hexdigest()

    def prepare(self, **kwargs):
        from experiment_runner.publication_dataset import prepare_dataset
        return prepare_dataset(self.source, self.output, rules=self.rules, **kwargs)

    def test_trials_share_values_without_merging_rows_failed_trials_or_source_bytes(self):
        self.publication('trials/first')
        self.publication('trials/second')
        self.publication('trials/failed', status='failed')
        excluded_sha = self.publication('trials/unrun', status='not_recorded')
        original = {str(p.relative_to(self.source)):p.read_bytes()
                    for p in self.source.rglob('*') if p.is_file()}

        self.prepare(excluded={'trials/unrun':excluded_sha})

        first = json.loads((self.output/'trials/first/results.jsonl').read_bytes())
        second = json.loads((self.output/'trials/second/results.jsonl').read_bytes())
        self.assertNotEqual(first['trial'], second['trial'])
        self.assertEqual(first['input'], second['input'])
        self.assertEqual(first['input'], first['evidence'])
        self.assertEqual(set(first['input']), {'$ref'})
        self.assertIs(type(first['input']['$ref']),int)
        self.assertGreater(first['input']['$ref'],0)
        self.assertEqual(json.loads((self.output/'trials/failed/results.jsonl').read_bytes())['status'], 'failed')
        self.assertFalse((self.output/'trials/unrun').exists())
        shared = [json.loads(line) for p in (self.output/'shared').glob('*.jsonl')
                  for line in p.read_bytes().splitlines()]
        self.assertEqual(len(shared), 1)
        self.assertEqual(shared[0]['value'], self.value)
        self.assertEqual(shared[0]['id'],first['input']['$ref'])
        self.assertEqual(shared[0]['sha256'],hashlib.sha256(canonical(self.value)).hexdigest())
        from experiment_runner.publication_dataset import restore_dataset, verify_dataset
        verify_dataset(self.output)
        restored = self.root/'restored'
        restore_dataset(self.output, restored)
        self.assertEqual({str(p.relative_to(restored)):p.read_bytes()
                          for p in restored.rglob('*') if p.is_file()},
                         {name:raw for name,raw in original.items()
                          if not name.startswith('trials/unrun/')})
        self.assertEqual(original,{str(p.relative_to(self.source)):p.read_bytes()
                                   for p in self.source.rglob('*') if p.is_file()})

    def test_unique_selected_value_stays_inline_without_a_shared_pool(self):
        self.publication('trial',rows=[dict(trial='unique',input=self.value)])
        original=(self.source/'trial/results.jsonl').read_bytes()
        result=self.prepare()
        self.assertEqual((self.output/'trial/results.jsonl').read_bytes(),original)
        self.assertFalse((self.output/'shared').exists())
        self.assertEqual(result['shared_values'],0)
        from experiment_runner.publication_dataset import restore_dataset
        restored=self.root/'restored'
        restore_dataset(self.output,restored,publication='trial')
        self.assertEqual((restored/'results.jsonl').read_bytes(),original)

    def test_repeated_values_stay_inline_when_reference_overhead_would_grow_them(self):
        self.publication('trial',rows=[dict(trial=i,input='x'*40) for i in range(3)])
        self.rules=[dict(format='trial-v1',file='results.jsonl',path=['input'],min_bytes=0)]
        original=(self.source/'trial/results.jsonl').read_bytes()
        result=self.prepare()
        self.assertEqual((self.output/'trial/results.jsonl').read_bytes(),original)
        self.assertEqual(result['shared_values'],0)

    def test_one_publication_restores_from_a_partial_download(self):
        self.publication('trials/first')
        value=dict(text='A different input. '*40)
        self.publication('trials/second',rows=[dict(trial='second',input=value,evidence=value)])
        self.prepare()
        first_id=json.loads((self.output/'trials/first/results.jsonl').read_bytes())['input']['$ref']
        for path in (self.output/'trials/second').iterdir():path.unlink()
        (self.output/'trials/second').rmdir()
        for path in (self.output/'shared').iterdir():
            if path.name!=f'{first_id % 256:02x}.jsonl':path.unlink()
        from experiment_runner.publication_dataset import restore_dataset,verify_dataset
        restored=self.root/'one-study'
        restore_dataset(self.output,restored,publication='trials/first')
        self.assertEqual({p.name:p.read_bytes() for p in restored.iterdir()},
                         {p.name:p.read_bytes() for p in (self.source/'trials/first').iterdir()})
        with self.assertRaisesRegex(ValueError,'extra or missing'):
            verify_dataset(self.output)

    def change_manifest(self, update):
        path=self.output/'dataset.json'
        value=json.loads(path.read_text());update(value)
        path.write_bytes(canonical(value)+b'\n')

    def rebind_packaged(self, name, *, shared=False):
        raw=(self.output/name).read_bytes()
        def update(value):
            value['shared' if shared else 'files'][name].update(
                sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
        self.change_manifest(update)

    def test_references_must_follow_manifest_rule_paths(self):
        self.publication('trial')
        self.prepare()
        self.change_manifest(lambda value:value['rules'][0].update(path=['unselected']))
        from experiment_runner.publication_dataset import verify_dataset
        with self.assertRaisesRegex(ValueError,'outside selection rule'):
            verify_dataset(self.output)

    def test_wildcards_preserve_repeated_rows_and_small_values(self):
        row=dict(trial='repeated',entries=[dict(input=self.value),dict(input='small')])
        self.publication('trial',rows=[row,row])
        self.rules=[dict(format='trial-v1',file='results.jsonl',path=['entries','*','input'])]
        self.prepare()
        rows=[json.loads(line) for line in (self.output/'trial/results.jsonl').read_bytes().splitlines()]
        self.assertEqual(rows[0],rows[1])
        self.assertEqual(rows[0]['entries'][1]['input'],'small')
        self.assertEqual(set(rows[0]['entries'][0]['input']),{'$ref'})
        from experiment_runner.publication_dataset import restore_dataset
        restored=self.root/'restored'
        restore_dataset(self.output,restored,publication='trial')
        self.assertEqual((restored/'results.jsonl').read_bytes(),(self.source/'trial/results.jsonl').read_bytes())

    def test_unknown_and_unselected_files_remain_byte_exact(self):
        self.publication('trial')
        raw=b'{ "intentionally" : "not canonical" }\r\n'
        (self.source/'trial/unselected.jsonl').write_bytes(raw)
        (self.source/'plain').mkdir()
        (self.source/'plain/results.jsonl').write_bytes(raw)
        (self.source/'plain/experiment.json').write_bytes(b'{"format":"unknown-v1"}')
        (self.source/'README.md').write_bytes(b'Ordinary file\r\n')
        self.prepare()
        for name in ('trial/unselected.jsonl','plain/results.jsonl','plain/experiment.json','README.md'):
            self.assertEqual((self.output/name).read_bytes(),(self.source/name).read_bytes())

    def test_source_binding_mismatch_and_noncanonical_rows_leave_no_output(self):
        self.publication('trial')
        path=self.source/'trial/results.jsonl'
        original=path.read_bytes()
        changed=json.loads(original);changed['trial']='different'
        for raw,message in ((canonical(changed)+b'\n','source binding'),
                            (b' '+original,'not canonical'),
                            (original.rstrip(b'\n'),'not canonical')):
            with self.subTest(message=message):
                path.write_bytes(raw)
                with self.assertRaisesRegex(ValueError,message):self.prepare()
                self.assertFalse(self.output.exists())
                self.assertFalse(list(self.root.glob('.publication-dataset-*')))

    def test_reserved_marker_is_rejected_even_outside_selected_fields(self):
        row=dict(input=self.value,unselected={'$ref':1})
        self.publication('trial',rows=[row])
        with self.assertRaisesRegex(ValueError,'reserved shared-reference marker'):self.prepare()
        self.assertFalse(self.output.exists())

    def test_missing_declared_selected_payload_is_not_packaged_as_complete(self):
        self.publication('trial')
        (self.source/'trial/results.jsonl').unlink()
        with self.assertRaisesRegex(ValueError,'missing declared selected file'):self.prepare()
        self.assertFalse(self.output.exists())

    def test_exclusions_require_exact_manifest_and_portable_roots(self):
        digest=self.publication('trial')
        for excluded in ({'trial':'0'*64},{'absent':digest},{'../trial':digest},
                         {'trial':digest,'trial/nested':digest}):
            with self.subTest(excluded=excluded):
                with self.assertRaises(ValueError):self.prepare(excluded=excluded)
                self.assertFalse(self.output.exists())
        self.prepare(excluded={'trial':digest})
        self.assertFalse((self.output/'trial').exists())

    def test_destinations_symlinks_and_rule_filenames_are_guarded(self):
        self.publication('trial')
        from experiment_runner.publication_dataset import prepare_dataset
        for destination in (self.source,self.source/'nested',self.root):
            with self.subTest(destination=destination):
                with self.assertRaisesRegex(ValueError,'overlaps source'):
                    prepare_dataset(self.source,destination,rules=self.rules)
        self.output.mkdir()
        (self.output/'unrelated').write_bytes(b'keep')
        with self.assertRaisesRegex(ValueError,'already exists'):self.prepare()
        self.assertEqual((self.output/'unrelated').read_bytes(),b'keep')
        (self.output/'unrelated').unlink();self.output.rmdir()
        alias=self.root/'alias';alias.symlink_to(self.source,target_is_directory=True)
        with self.assertRaisesRegex(ValueError,'symlinks'):
            prepare_dataset(alias,self.output,rules=self.rules)
        (self.source/'alias').symlink_to(self.source/'trial/results.jsonl')
        with self.assertRaisesRegex(ValueError,'regular files'):self.prepare()
        (self.source/'alias').unlink()
        for filename in ('../results.jsonl','nested/results.jsonl'):
            self.rules[0]['file']=filename
            with self.assertRaisesRegex(ValueError,'selection rule'):self.prepare()

    def test_missing_and_wrong_shared_values_fail_atomically(self):
        self.publication('trial');self.prepare()
        from experiment_runner.publication_dataset import restore_dataset,verify_dataset
        shard=next((self.output/'shared').iterdir())
        original=shard.read_bytes();shard.unlink()
        restored=self.root/'restored'
        with self.assertRaisesRegex(ValueError,'missing shared shard'):
            restore_dataset(self.output,restored,publication='trial')
        self.assertFalse(restored.exists())
        shard.write_bytes(original)
        value=json.loads(original);value['value']['text']='changed'
        shard.write_bytes(canonical(value)+b'\n')
        self.rebind_packaged(shard.relative_to(self.output).as_posix(),shared=True)
        with self.assertRaisesRegex(ValueError,'shared value hash differs'):verify_dataset(self.output)

    def test_shared_ids_are_required_unique_and_in_the_correct_bucket(self):
        self.publication('trial');self.prepare()
        from experiment_runner.publication_dataset import verify_dataset
        shard=next((self.output/'shared').iterdir())
        original=shard.read_bytes();manifest=(self.output/'dataset.json').read_bytes()
        row=json.loads(original)
        missing=dict(row);del missing['id']
        wrong_bucket={**row,'id':row['id']+1}
        other={**row,'value':dict(text='a different full scientific value'*30)}
        other['sha256']=hashlib.sha256(canonical(other['value'])).hexdigest()
        for rows,message in (([missing],'invalid shared value record'),
                             ([wrong_bucket],'bucket differs'),
                             ([row,other],'duplicate shared value')):
            with self.subTest(message=message):
                (self.output/'dataset.json').write_bytes(manifest)
                shard.write_bytes(b''.join(canonical(value)+b'\n' for value in rows))
                self.rebind_packaged(shard.relative_to(self.output).as_posix(),shared=True)
                self.change_manifest(lambda value:value['shared'][shard.relative_to(self.output).as_posix()].update(records=len(rows)))
                with self.assertRaisesRegex(ValueError,message):verify_dataset(self.output)

    def test_missing_reference_and_wrong_reconstruction_are_rejected(self):
        self.publication('trial');self.prepare()
        from experiment_runner.publication_dataset import restore_dataset,verify_dataset
        path=self.output/'trial/results.jsonl'
        original=path.read_bytes()
        value=json.loads(original);value['input']={'$ref':999999}
        path.write_bytes(canonical(value)+b'\n');self.rebind_packaged('trial/results.jsonl')
        with self.assertRaisesRegex(ValueError,'missing shared reference'):verify_dataset(self.output)
        value=json.loads(original);value['trial']='different-scientific-identity'
        path.write_bytes(canonical(value)+b'\n');self.rebind_packaged('trial/results.jsonl')
        restored=self.root/'restored'
        with self.assertRaisesRegex(ValueError,'original reconstruction'):
            restore_dataset(self.output,restored,publication='trial')
        self.assertFalse(restored.exists())

    def test_extra_missing_and_traversing_manifest_files_are_rejected(self):
        self.publication('trial');self.prepare()
        from experiment_runner.publication_dataset import verify_dataset
        extra=self.output/'extra';extra.write_bytes(b'unknown')
        with self.assertRaisesRegex(ValueError,'extra or missing'):verify_dataset(self.output)
        extra.unlink()
        self.change_manifest(lambda value:value['files'].update({'../escape':value['files']['trial/results.jsonl']}))
        with self.assertRaisesRegex(ValueError,'portable source-relative'):verify_dataset(self.output)

    def test_memory_does_not_grow_with_repeated_input_bytes(self):
        value={'text':'shared-value '*600}
        directory=self.source/'trial';directory.mkdir()
        path=directory/'results.jsonl'
        digest=hashlib.sha256();size=0;count=2000
        with path.open('wb') as stream:
            for trial in range(count):
                raw=canonical(dict(trial=trial,input=value))+b'\n'
                digest.update(raw);size+=len(raw);stream.write(raw)
        (directory/'experiment.json').write_bytes(canonical(dict(format='trial-v1',files={
            'results.jsonl':dict(sha256=digest.hexdigest(),bytes=size,records=count)}))+b'\n')
        tracemalloc.start()
        try:
            result=self.prepare()
            _,peak=tracemalloc.get_traced_memory()
        finally:tracemalloc.stop()
        self.assertEqual(result['shared_values'],1)
        self.assertEqual(result['references'],count)
        self.assertLess(peak,8*1024*1024)


if __name__ == '__main__':
    unittest.main()
