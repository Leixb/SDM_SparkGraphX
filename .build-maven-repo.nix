{ lib
, stdenv
, maven
}:

stdenv.mkDerivation {
  name = "maven-repository";
  buildInputs = [ maven ];
  src = ./.; # or fetchFromGitHub, cleanSourceWith, etc
  buildPhase = ''
    runHook preBuild

    mvn package -Dmaven.repo.local=$out

    runHook postBuild
  '';

  # keep only *.{pom,jar,sha1,nbm} and delete all ephemeral files with lastModified timestamps inside
  installPhase = ''
    runHook preInstall

    find $out -type f \
      -name \*.lastUpdated -or \
      -name resolver-status.properties -or \
      -name _remote.repositories \
      -delete

    runHook postInstall
  '';

  # don't do any fixup
  dontFixup = true;
  outputHashAlgo = "sha256";
  outputHashMode = "recursive";
  # replace this with the correct SHA256
  outputHash = "1yvaj0dwa661p8nbg5188a9sp9zfdwcy51032iq4pgmy1gq11140";
}
