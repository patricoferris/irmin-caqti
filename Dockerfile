FROM ocaml/opam:alpine-3.16-ocaml-5.0 as build
RUN sudo ln -f /usr/bin/opam-2.1 /usr/bin/opam
RUN cd ~/opam-repository && git pull origin -q master && git reset --hard 0e970838e3e70808f882954d61828acc4ce30b06 && opam update
RUN sudo apk add gmp-dev libffi-dev linux-headers pkgconf
COPY --chown=opam irmin-caqti-lwt.opam /src/
RUN opam pin -yn /src/
WORKDIR /src
RUN opam install -y --deps-only --with-test .
ADD --chown=opam . .
RUN opam exec -- dune build
