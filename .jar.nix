{ lib
, stdenv
, maven
, jre8
, makeWrapper
, callPackage
}:

let
  repository = callPackage ./.build-maven-repo.nix { };
  classpath =  builtins.concatStringsSep ":" (builtins.map (x: repository + x) ((lib.splitString "\n") (builtins.readFile ./.classpath.list)));
in

stdenv.mkDerivation rec {
  pname = "spark_graphs_II";
  version = "0.0.1-SNAPSHOT";

  src = ./.;

  nativeBuildInputs = [ maven makeWrapper ];

  buildPhase = ''
    runHook preBuild

    echo "Using repository ${repository}"
    mvn --offline -Dmaven.repo.local=${repository} package;

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/share/java
    cp -Rv target/* $out/share/java

    mkdir -p $out/bin

    install -Dm644 target/${pname}-${version}.jar $out/share/java/${pname}-${version}.jar

    makeWrapper ${jre8}/bin/java $out/bin/${pname} \
      --add-flags "-classpath $out/share/java/${pname}-${version}.jar:${classpath}" \
      --add-flags "Main"

    runHook postInstall
  '';
}
