final: prev: {

  rocksdb = prev.rocksdb.overrideAttrs (oldAttrs: rec {
    version = "10.4.2";

    src = final.fetchFromGitHub {
      owner = "facebook";
      repo = oldAttrs.pname;
      rev = "v${version}";
      hash = "sha256-mKh6zsmxsiUix4LX+npiytmKvLbo6WNA9y4Ns/EY+bE=";
    };
  });
}
