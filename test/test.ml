

let store = Irmin_test.store (module Irmin_caqti_lwt.Make) (module Irmin.Metadata.None)

let drop_table (module Db : Caqti_lwt.CONNECTION) s =
  let open Caqti_type in
  let open Caqti_request.Infix in
  let q = unit ->. unit @@ ("DROP TABLE " ^ s) in
  Db.exec q ()

let clean ~config =
  let open Lwt.Syntax in
  let module C = Irmin.Backend.Conf in
  let uri = C.get config Irmin_caqti_lwt.Conf.uri in
  let* _ =
    Caqti_lwt.with_connection uri @@ fun db ->
    let* _s = drop_table db "append_only" in
    let* _s = drop_table db "atomic" in
    Lwt.return (Ok ())
  in
    Lwt.return_unit

let suite config =
  Irmin_test.Suite.create ~clean ~name:"FS.UNIX" ~store ~config ()

let () =
  Unix.sleep 4;
  let main () =
    let config = Irmin_caqti_lwt.config (Uri.of_string "postgresql://bactrian:bactrian@postgresql:5432") in
    Irmin_test.Store.run "irmin-caqti.unix" ~slow:false ~sleep:Lwt_unix.sleep
       ~misc:[]
       [ (`Quick, suite config) ]
  in
  Lwt_main.run (main ())