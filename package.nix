{ lib, stdenvNoCC, buildNpmPackage, makeWrapper, python3, nodejs_24 }:
let
  runtime = python3.withPackages (ps: [ ps.tiktoken ps.numpy ]);
  nodeDependencies = buildNpmPackage {
    pname = "experiment-runner-node-dependencies";
    version = "1.0.0";
    src = ./experiment_runner/frontier_runner;
    npmDepsHash = "sha256-IDc3g7+TDGLD0gmJFvtrJR+qZTDs/BICL4C40dvDuNI=";
    nodejs = nodejs_24;
    dontNpmBuild = true;
    installPhase = ''
      mkdir -p $out/lib
      cp -r node_modules $out/lib/node_modules
    '';
  };
in stdenvNoCC.mkDerivation {
  pname = "experiment-runner";
  version = "1.2.1";
  src = lib.cleanSource ./.;
  nativeBuildInputs = [ makeWrapper ];
  dontBuild = true;
  installPhase = ''
    mkdir -p $out/lib/experiment-runner $out/bin
    cp -r experiment_runner $out/lib/experiment-runner/
    ln -s ${nodeDependencies}/lib/node_modules $out/lib/experiment-runner/experiment_runner/frontier_runner/node_modules
    patchShebangs $out/lib/experiment-runner/experiment_runner/run-model.sh
    makeWrapper ${runtime}/bin/python3 $out/bin/experiment-runner \
      --add-flags "-m experiment_runner.cli" \
      --prefix PYTHONPATH : $out/lib/experiment-runner \
      --prefix PATH : ${lib.makeBinPath [ nodejs_24 runtime ]}
  '';
  meta = {
    description = "One executor for preserved prompt experiments";
    mainProgram = "experiment-runner";
    platforms = lib.platforms.linux;
  };
}
