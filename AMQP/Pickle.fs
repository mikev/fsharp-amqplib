namespace AMQP

//
// For further details on Pickler theory see:
// Functional PEARL Pickler Combinators paper by Andrew Kennedy
// http://research.microsoft.com/pubs/64036/picklercombinators.pdf
//

module CorePickle =

    open System
    open System.IO
    open System.Net
    open System.Text

    type WriterState = System.IO.BinaryWriter
    type ReaderState = System.IO.BinaryReader

    type pickler<'T> = 'T -> WriterState -> unit
    type unpickler<'T> = ReaderState -> 'T

    let p_char (b:char) (st:WriterState) = st.Write(b)
    let u_char (st:ReaderState) = st.ReadChar()

    let p_byte (b:int) (st:WriterState) = st.Write (byte b)
    let u_byte (st:ReaderState) = (int (st.ReadByte()))

    let p_bool (b:bool) (st:WriterState) = p_byte (if b then 1 else 0) st
    let u_bool st = let b = u_byte st in (b = 1) 

    let p_int32 (n:int32) (st:WriterState) =
        st.Write (int32 (IPAddress.HostToNetworkOrder(n)))
    let u_int32 (st:ReaderState) =
        IPAddress.NetworkToHostOrder(st.ReadInt32())

    let p_int c st = p_int32 c st
    let p_int8 (i:sbyte) st = p_int32 (int32 i) st
    let p_uint8 (i:byte) st = p_byte (int i) st

    let p_int16 (i:int16) (st:WriterState) =
        st.Write (int16 (IPAddress.HostToNetworkOrder(int16 i)))
    let u_int16 (st:ReaderState) =
        IPAddress.NetworkToHostOrder(st.ReadInt16())

    let p_uint16 (x:uint16) (st:WriterState) =
         st.Write (uint16 (IPAddress.HostToNetworkOrder(int16 x)))
    let u_uint16 (st:ReaderState) =
        (uint16 (IPAddress.NetworkToHostOrder(st.ReadInt16())))

    let p_uint32 (x:uint32) st = p_int32 (int32 x) st

    let p_int64 (i:int64) (st:WriterState) =
        st.Write (IPAddress.HostToNetworkOrder(int64 i))
    let u_int64 (st:ReaderState) =
        IPAddress.NetworkToHostOrder(st.ReadInt64())

    let p_uint64 (x:uint64) st = p_int64 (int64 x) st

    let p_octet (i:int) st = p_byte i st

    let p_short (i:int) st = p_uint16 (uint16 i) st
    let u_short (st:ReaderState) = u_uint16 st

    let p_long (i:int) st = p_int32 i st
    let u_long st = u_int32 st

    let p_longlong (i:int64) st = p_int64 i st
    let u_longLong st = u_int64 st

    let p_shortString (str:string) (st:WriterState) = st.Write str
    let u_shortString (st:ReaderState) =
        st.ReadString()

    let p_zeroString (str:string) (st:WriterState) =
        let chars = str.ToCharArray()
        p_byte 0 st
        st.Write chars

    let p_emptyTable st = p_int32 0 st

    let p_bytes (s:byte[]) (st:WriterState) =
        p_long s.Length st
        st.Write s
    let u_bytes (st:ReaderState) =
        let len = u_long st
        st.ReadBytes(len)

    let p_longString (str:string) (st:WriterState) =
        let len = str.Length
        p_long len st
        let bytes = Encoding.ASCII.GetBytes(str)
        p_bytes bytes st
    let u_longString (st:ReaderState) =
        failwith "Not implemented"
        let len = u_long st
        u_bytes st  

    let p_fieldname str st =
        p_shortString str st
    let u_fieldname st =
        u_shortString st
        
    let p_stringfieldvalue str st =
        p_char 'S' st
        p_longString str st
    let u_stringfieldvalue st =
        let theType = u_char st
        if (theType <> 'S') then failwith "Expected 'S'"
        u_longString st

    let p_stringfieldvaluepair (a:string) (b:string) st =
        p_fieldname a st
        p_stringfieldvalue b st

    let u_payload (st:ReaderState) =
        let t = u_byte st
        let chan = u_short st
        let pLen = u_long st
        let payload = st.ReadBytes( pLen )
        let eof = u_byte st
        if (eof <> 0xce) then failwith "Unframed packet"
        payload

    let u_payload2 (st:ReaderState) =
        let t = u_byte st
        let chan = u_short st
        let pLen = u_long st
        let payload = st.ReadBytes( pLen )
        let eof = u_byte st
        if (eof <> 0xce) then failwith "Unframed packet"
        (t, chan, pLen, payload)

    let inline p_tup2 p1 p2 (a,b) (st:WriterState) = (p1 a st : unit); (p2 b st : unit)
    let inline p_tup3 p1 p2 p3 (a,b,c) (st:WriterState) = (p1 a st : unit); (p2 b st : unit); (p3 c st : unit)

    let inline  u_tup2 p1 p2 (st:ReaderState) = let a = p1 st in let b = p2 st in (a,b)
    let inline  u_tup3 p1 p2 p3 (st:ReaderState) =
      let a = p1 st in let b = p2 st in let c = p3 st in (a,b,c)

    let ByteToHex bytes = 
        bytes 
        |> Array.map (fun (x : byte) -> System.String.Format("{0:X2}", x))
        |> String.concat System.String.Empty

    let writeSeq (writer:BinaryWriter) str =

        // map hex ascii to binary
        let hexCharToByte value =
            match value with
            | _ when (value >= '0' && value <= '9') ->
                (byte value - byte '0')
            | _ when (value >= 'a' && value <= 'f') ->
                (byte value - byte 'a' + byte 10)
            | _ when (value >= 'A' && value <= 'F') ->
                (byte value - byte 'A' + byte 10)
            | _ -> byte 0

        // transform elem -> (elem * elem)
        let pairs seq =
            seq
            |> Seq.pairwise
            |> Seq.mapi (fun i x -> if i%2=0 then Some(x) else None)
            |> Seq.choose id

        // transform string to binary
        let stringToByteSeq input =
            input
            |> Seq.map (fun a -> hexCharToByte a)

        // put pairs into binary int
        let pairsToInt (x:seq<byte * byte>) =
            x
            |> Seq.map (fun (a,b) -> ((a <<< 4) ||| b))

        str
        |> stringToByteSeq
        |> pairs
        |> pairsToInt
        |> Seq.iter (fun (a:byte) -> writer.Write a)

