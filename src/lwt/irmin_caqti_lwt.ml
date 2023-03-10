open Lwt.Syntax

module UTF8_codec = struct
  (* Copyright (C) 2015, Thomas Leonard.
   * See irmin-indexedb README file for details. *)
  let tail s i =
    String.sub s i (String.length s - i)

  (* From https://github.com/mirage/ezjsonm.
   * Copyright (c) 2013 Thomas Gazagnaire <thomas@gazagnaire.org> *)
  let is_valid_utf8 str =
    try
      Uutf.String.fold_utf_8 (fun () _ -> function
        | `Malformed _ -> raise Exit
        | _ -> ()
      ) () str;
      true
    with Exit -> false

  module B64 = struct
    let encode s =
      match Base64.encode s with
      | Ok x -> x
      | Error (`Msg m) -> failwith m    (* Encoding can't really fail *)

    let decode s =
      match Base64.decode s with
      | Ok x -> x
      | Error (`Msg m) -> failwith ("B64.decode: " ^ m)
  end

  let encode s = B64.encode s

  let decode s = B64.decode s
end

let src = Logs.Src.create "irmin-caqti.lwt" ~doc:"Logging for Irmin Caqti backend with Lwt"
module Log = (val Logs.src_log src : Logs.LOG)

module Caqti_of_irmin (V : Irmin.Type.S) = struct
  type t = V.t

  (* XXX: Could be cleverer and do some 'a Repr.t -> 'a Caqti_type.t function
     that might preserve some nice SQLy lookups in the underlying store? Would
     need to exist in Repr as it doesn't expose the underlying types.

     We also use JSON for UTF8 reasons. *)

  let to_bin_string s =
    Irmin.Type.(unstage @@ to_bin_string V.t) s |> UTF8_codec.encode |> fun s ->
    Result.ok s

  let of_bin_string s =
    Irmin.Type.(unstage @@ of_bin_string V.t) (UTF8_codec.decode s) |> function
    | Ok v -> Ok v
    | Error (`Msg m) -> Error ("Caqti serialisation failed: " ^ m ^ " -- " ^ (UTF8_codec.decode s) ^ string_of_bool (s = ""))

  let t =
    Caqti_type.custom ~encode:to_bin_string ~decode:of_bin_string Caqti_type.string
end

let to_msg_error = function
    | Ok v -> v
    | Error c -> failwith (Fmt.to_to_string Caqti_error.pp c)

module Query (T : sig val table : string end) (K : Irmin.Type.S) (V : Irmin.Type.S) = struct
  open Caqti_request.Infix
  open Caqti_type.Std

  module Kt = struct
    type t = K.t
    let encode v = Irmin.Type.to_string K.t v |> Result.ok
    let decode s = match Irmin.Type.of_string K.t s with
     | Ok _ as v -> v
     | Error (`Msg m) -> Error ("Key failed: " ^ m)

    let t = custom ~encode ~decode string
  end
  module Ct = Caqti_of_irmin (V)

  let lookup_query =
    ((->*) ~oneshot:true) Kt.t Ct.t @@
    Fmt.str "SELECT value FROM %s WHERE key = ?" T.table

  let lookup (module Db : Caqti_lwt.CONNECTION) k =
    let+ res = Db.fold lookup_query (fun v acc -> v :: acc) k [] in
    let res = to_msg_error res in
    [%log.debug "lookup2 %a %i (%a)" (Irmin.Type.pp K.t) k (List.length res) (Fmt.list @@ Irmin.Type.pp V.t) res];
    try Some (List.hd res) with _ -> None

  let member_query =
    (Kt.t ->! int) ~oneshot:true @@
    Fmt.str "SELECT COUNT(1) FROM %s WHERE key = ?" T.table

  let member (module Db : Caqti_lwt.CONNECTION) k =
    let+ res = Db.find member_query k in
    Result.map (fun v -> v >= 1) res |> to_msg_error

  let insert_query =
    (tup2 Kt.t Ct.t ->. unit) ~oneshot:true @@
    Fmt.str "INSERT INTO %s (key, value) VALUES (?, ?)" T.table

  let insert (module Db : Caqti_lwt.CONNECTION) k v =
    [%log.debug "insert %a %a" (Irmin.Type.pp K.t) k (Irmin.Type.pp V.t) v];
    let+ res = Db.exec insert_query (k, v) in
    to_msg_error res

  let keys_query =
    (unit ->* Kt.t) ~oneshot:true @@
    Fmt.str "SELECT key FROM %s" T.table

  let keys (module Db : Caqti_lwt.CONNECTION) =
    let+ res = Db.fold keys_query (fun v acc -> v :: acc) () [] in
    [%log.debug "keyyyyyys %a" Fmt.(list (Irmin.Type.pp K.t)) (Result.get_ok res)];
     res |> to_msg_error

  let update_query =
    (tup2 Ct.t Kt.t ->. unit) ~oneshot:true @@
    Fmt.str "UPDATE %s SET value = ? WHERE key = ?" T.table

  let update (module Db : Caqti_lwt.CONNECTION) k v =
    [%log.debug "update %a %a" (Irmin.Type.pp K.t) k (Irmin.Type.pp V.t) v];
    let+ res = Db.exec update_query (v, k) in
    to_msg_error res

  let remove_query =
    (Kt.t ->. unit) ~oneshot:true @@
    Fmt.str "DELETE FROM %s WHERE key = ?" T.table

  let remove (module Db : Caqti_lwt.CONNECTION) k =
    [%log.debug "removing %a" (Irmin.Type.pp K.t) k];
    let* res = Db.exec_with_affected_count remove_query k in
    let _ = to_msg_error res in
    (* assert (r = 1); *)
    Lwt.return_unit
end

module Conf = struct
  include Irmin.Backend.Conf
  let spec = Irmin.Backend.Conf.Spec.v "irmin-caqti"

  let uri_type =
    Irmin.Type.map Irmin.Type.string Uri.of_string Uri.to_string

  let uri = Irmin.Backend.Conf.key ~spec "uri" uri_type (Uri.of_string "http://localhost:8080")
end

let config v = Conf.(verify (add (empty Conf.spec) Conf.uri v))

module Aux (K : Irmin.Type.S) (V : Irmin.Type.S) = struct
  type 'a t = Caqti_lwt.connection Lwt_pool.t
  type key = K.t
  type value = V.t

  let create_query name =
    let open Caqti_type in
    let open Caqti_request.Infix in
    unit ->. unit @@
    Fmt.str "CREATE TABLE IF NOT EXISTS %s (key text NOT NULL, value text NOT NULL)" name

  let create pool name =
    Lwt_pool.use pool @@ fun (module Db : Caqti_lwt.CONNECTION) ->
    let+ res = Db.exec (create_query name) () in
    to_msg_error res

  let v config =
    let module C = Irmin.Backend.Conf in
    let uri = C.get config Conf.uri in
    let pool = Lwt_pool.create 1 (fun () -> let* conn = Caqti_lwt.connect uri in Caqti_lwt.or_fail conn) in
    let+ () = create pool "append_only" in
    pool

  module Q = Query (struct let table = "append_only" end) (K) (V)

  let mem t k =
    Lwt_pool.use t @@ fun db ->
    [%log.debug "mem %a" (Irmin.Type.pp K.t) k];
    Q.member db k

  let find t k =
    Lwt_pool.use t @@ fun db ->
    [%log.debug "lookup append_only %a" (Irmin.Type.pp K.t) k];
    Q.lookup db k
end

module Append_only : Irmin.Append_only.Maker = functor
  (K : Irmin.Type.S)
  (V : Irmin.Type.S) -> struct

  include Aux (K) (V)

  let add t key value =
    Lwt_pool.use t @@ fun db ->
    [%log.debug "add %a" (Irmin.Type.pp K.t) key];
    Q.insert db key value

  let batch t f =
    let+ x = Lwt.catch (fun () -> f t)
      (fun exn -> raise exn)
    in
    x

  let close t =
    Lwt_pool.use t @@ fun (module Db : Caqti_lwt.CONNECTION) ->
    Db.disconnect ()
end

module Content_addressable = Irmin.Content_addressable.Make (Append_only)

module Atomic_write : Irmin.Atomic_write.Maker = functor
  (K : Irmin.Type.S)
  (V : Irmin.Type.S) -> struct

  module L = Irmin.Backend.Lock.Make (K)
  module A = Aux (K) (V)
  module App = Append_only(K)(V)

  module W = Irmin.Backend.Watch.Make (K) (V)

  let lock = L.v ()

  type t = { t : unit A.t; w : W.t; lock : L.t }

  type key = A.key
  type value = A.value
  type watch = W.watch
  let watches = W.v ()

  let v config =
    let module C = Irmin.Backend.Conf in
    let uri = C.get config Conf.uri in
    let pool = Lwt_pool.create 16 (fun () -> let* conn = Caqti_lwt.connect uri in Caqti_lwt.or_fail conn) in
    let+ () = A.create pool "atomic" in
    { t = pool; w = watches; lock }

  module Q = Query (struct let table = "atomic" end) (K) (V)

  let mem { t; _ } k =
    Lwt_pool.use t @@ fun t ->
    [%log.debug "mem %a" (Irmin.Type.pp K.t) k];
    Q.member t k

  let find { t; _ } k =
    Lwt_pool.use t @@ fun t ->
    [%log.debug "lookup atomic %a" (Irmin.Type.pp K.t) k];
    Q.lookup t k

  let watch_key t key = W.watch_key t.w key
  let watch t = W.watch t.w
  let unwatch t = W.unwatch t.w

 let list { t; _ } =
  Lwt_pool.use t @@ fun t ->
  [%log.debug "fetch-keys"];
  Q.keys t

 (* TODO: Yes can probably do better with transactions and SQL if exists logic *)
 let set ({ t = conn; w; _ } as t) k v =
  L.with_lock lock k @@ fun () ->
  Lwt_pool.use conn @@ fun conn ->
  [%log.debug "SETTING VALUE %a %a" (Irmin.Type.pp K.t) k (Irmin.Type.pp V.t) v];
  let* exists = mem t k in
  let update =
    if exists then Q.update conn k v
    else Q.insert conn k v
  in
  let* () = update in
  W.notify w k (Some v)

 let remove { t; w; _ } k =
  L.with_lock lock k @@ fun () ->
  Lwt_pool.use t @@ fun t ->
  [%log.debug "remove %a" (Irmin.Type.pp K.t) k];
  let* () = Q.remove t k in
  W.notify w k None

 (* TODO: This isn't great atomicity... logic should probably in SQL.. *)
 let test_and_set ({ t; w; lock } as db) k ~test ~set =
  L.with_lock lock k @@ fun () ->
  Lwt_pool.use t @@ fun t ->
  [%log.debug "test_and_set %a %a" (Irmin.Type.pp K.t) k Fmt.(option @@ Irmin.Type.pp V.t) set];
  let fn () =
    let* old = find db k in
    match old, test, set with
    | None, None, Some v ->
      let* () = Q.insert t k v in
      Lwt.return_true
    | Some v, Some v', s -> (
      if not (Irmin.Type.(unstage (equal V.t) v v')) then Lwt.return_false
      else
      match s with
      | Some set ->
        let* () = Q.update t k set in
        Lwt.return_true
      | None ->
        let* () = Q.remove t k in
        Lwt.return_true
    )
    | _ -> Lwt.return_false
  in
  let+ x = Lwt.catch (fun () ->
    let* b = fn () in
    if b then (let+ () = W.notify w k set in b) else Lwt.return b)
    (fun exn -> raise exn)
  in
  x

 let clear _ =
  [%log.debug "clear"];
  Lwt.return_unit
 let close { t; _ } =
  Lwt_pool.use t @@ fun (module Db) ->
  [%log.debug "close"];
  Db.disconnect ()
end

module Make = Irmin.Maker (Content_addressable) (Atomic_write)