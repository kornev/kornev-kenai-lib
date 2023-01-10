{
  description = "A Clojure development environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.11";

  outputs = { self, nixpkgs }:
    let pkgs = nixpkgs.legacyPackages.x86_64-linux;
    in {
      devShells.x86_64-linux.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          (clojure.override { jdk = jdk8; })
          (leiningen.override { jdk = jdk8; })
          jdk8
        ];
        shellHook = ''
          export JAVA_HOME="${pkgs.jdk8.home}"
          export LEIN_REPL_HOST=127.0.0.1
          export LEIN_REPL_PORT=65050
          export LEIN_NO_USER_PROFILES
        '';
      };
    };
}

