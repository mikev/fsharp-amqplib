// This file is a script that can be executed with the F# Interactive.  
// It can be used to explore and test the library project.
// Note that script files will not be part of the project build.

#load "Pickle.fs"
open AMQPClient
open AMQPClient.CorePickle

open System
open System.IO
open System.Net

type WriterState = System.IO.BinaryWriter
type ReaderState = System.IO.BinaryReader

type pickler<'T> = 'T -> WriterState -> unit

let bytePX (b: byte) (st: WriterState) = st.Write(b)
let byteP = bytePX:pickler<byte>


//#load "SocketClient.fs"
open AMQPClient

open System.Net
open System.IO
open System.Net.Sockets
open System.Text

let write8 (writer:BinaryWriter) (v:int) =
    writer.Write(byte v)
let write16 (writer:BinaryWriter) (v:int) =
   let v2 = IPAddress.HostToNetworkOrder((int16 v))
   writer.Write v2
let write32 (writer:BinaryWriter) (v:int32) =
   let v2 = IPAddress.HostToNetworkOrder(v)
   writer.Write v2
let write64 (writer:BinaryWriter) (v:int64) =
   let v2 = IPAddress.HostToNetworkOrder(v)
   writer.Write v2
let writeShortString (writer:BinaryWriter) (str:string) =
    let len = str.Length
    writer.Write str
let writeByteArray (writer:BinaryWriter) (b:byte[]) =
    writer.Write b

let sendCommand (socket:Socket) string =
    let memStream = new MemoryStream(1024)
    use bw = new BinaryWriter(memStream)
    AMQPClient.CorePickle.writeSeq bw string
    let sendBuffer = memStream.ToArray()
    let ret = socket.Send( sendBuffer )
    let replyBuffer = Array.zeroCreate<byte> 2048
    let len = socket.Receive(replyBuffer)
    printfn "Socket received: %d bytes" (len)
    replyBuffer

let sendTCPCommand (socket:Socket) (writer:BinaryWriter) string =
    AMQPClient.CorePickle.writeSeq writer string
    writer.Flush()
    let replyBuffer = Array.zeroCreate<byte> 2048
    let len = socket.Receive(replyBuffer)
    printfn "Socket received: %d bytes" (len)
    replyBuffer

let applyTCPCommand (socket:Socket) (writer:BinaryWriter) f =
    f writer
    writer.Flush()
    let replyBuffer = Array.zeroCreate<byte> 2048
    let len = socket.Receive(replyBuffer)
    printfn "Socket received: %d bytes" (len)
    replyBuffer

let applyTCPNoReply (socket:Socket) (writer:BinaryWriter) f =
    f writer
    writer.Flush()

let sendTCPBinary (socket:Socket) (writer:BinaryWriter) (b:byte[]) =
    AMQPClient.CorePickle.p_bytes b writer
    writer.Flush()
    let replyBuffer = Array.zeroCreate<byte> 2048
    let len = socket.Receive(replyBuffer)
    printfn "Socket received: %d bytes" (len)
    replyBuffer

let sendTCPNoReply (socket:Socket) (writer:BinaryWriter) string =
    AMQPClient.CorePickle.writeSeq writer string
    writer.Flush()

let port = 5672
//let socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
//let hostName = "rabbit5.np.wc1.yellowpages.com"
//let ipHostEntry = Dns.GetHostEntry(hostName)
//printfn "Server address: %s" (ipHostEntry.AddressList.[0].ToString())
//
//socket.Connect(hostName,port)
//printfn "Socket connected to: %s" (socket.RemoteEndPoint.ToString() )
//
//let memStream = new MemoryStream(8)
//let bw = new BinaryWriter(memStream)
//bw.Write( byte 'A' )
//bw.Write( byte 'M' )
//bw.Write( byte 'Q' )
//bw.Write( byte 'P' )
//bw.Write( byte 1 )
//bw.Write( byte 1 )
//bw.Write( byte 8 )
//bw.Write( byte 0 )
//let buffer1 = [| 0uy .. 255uy |]
//let buffer2 = Array.zeroCreate<byte> 1024
//printfn "Sending data..."
//let buffer3 = memStream.ToArray()
//socket.Send( buffer3 )
//let len = socket.Receive(buffer2)
//printfn "Socket received: %d bytes" (len)

// FYI.  C# dotnet source code is 24,000 lines of code
// FYI.  C# dotnet source code is 32,000 lines of code (includes WCF and examples)

#load "Pickle.fs"
let cmd = (AMQPClient.Client.ContentHeader 60 (int64 10))
let ms1 = new MemoryStream(1024)
let bw1 = new BinaryWriter(ms1)
cmd bw1
let out1 = ms1.ToArray()
let hex1 = ByteToHex out1
let str1 = AMQPClient.Client.ContentHeaderStr

let counter =
    new MailboxProcessor<int64>(fun inbox ->
        let rec loop n =
            async { printfn "n = %d, waiting..." n
                    let! msg = inbox.Receive()
                    return! loop (n+msg) }
        loop 0)

counter.Start()
counter.Post(int64 1)
counter.Post(int64 2)
counter.Post(int64 1)
let res = counter.Receive(100)

#load "Pickle.fs"
#load "AMQP.fs"
open AMQPClient
open AMQPClient.CorePickle
open System.IO

let gp = AMQPClient.Client.BasicGetFullStr
let ms = new MemoryStream(256)
let bw = new BinaryWriter(ms)
AMQPClient.CorePickle.writeSeq bw gp
bw.Flush()
ms.Seek(int64 0, SeekOrigin())
let reader = new BinaryReader(ms)

u_payload reader
u_payload reader
let o2 = u_payload reader

let t = u_byte reader
let chan = u_short reader
let pLen = u_long reader
reader.ReadBytes( pLen )
let eof = u_byte reader
let t2 = u_byte reader
let chan2 = u_short reader
let pLen2 = u_long reader
reader.ReadBytes( pLen2 )
let eof2 = u_byte reader
let t3 = u_byte reader
let chan3 = u_short reader
let pLen3 = u_long reader
let output = reader.ReadBytes( pLen3 )



#load "Pickle.fs"
#load "AMQP.fs"
open AMQPClient
open AMQPClient.CorePickle

let amqpFun = (fun inbox ->
                   let rec loop n =
                        async { printfn "hello %d" n }
                   loop 0
                )

let tcpHostName = "tdbrabbit1.np.ev1.yellowpages.com"
let tcpPort = 5672
let queueName = "foobar"
let amqp = new AMQPClient.AMQP( amqpFun, tcpHostName, tcpPort )
amqp.Queue <- queueName
amqp.Connect()
amqp.PostText "HelloWorld2"

amqp.TxSelect
amqp.PostText "HelloWorld3"
amqp.TxCommit

let reply = amqp.Receive()

let hello = "HelloWorld"
let helloArray = Encoding.ASCII.GetBytes(hello)
amqp.PostText "HelloWorld"

amqp.PostTextWithTransact("HelloWorldTx")

#load "Pickle.fs"
open AMQPClient
open AMQPClient.CorePickle


let tcpHostName = "tdbrabbit1.np.ev1.yellowpages.com"
let tcpPort = 5672
let tcpSocket = new TcpClient(AddressFamily.InterNetwork)
tcpSocket.Connect(tcpHostName, tcpPort)
// disable Nagle's algorithm, for more consistently low latency 
tcpSocket.NoDelay <- true
tcpSocket.ReceiveTimeout <- 120
tcpSocket.SendTimeout <- 120
let netstream = tcpSocket.GetStream()
let m_reader = new BinaryReader(new BufferedStream(netstream))
let m_writer = new BinaryWriter(new BufferedStream(netstream))
let command = applyTCPCommand tcpSocket.Client m_writer
let commandNoReply = applyTCPNoReply tcpSocket.Client m_writer
sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.ProtocolHeader
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.StartOKStr
command (AMQPClient.Client.StartOK "guest" "guest")
//sendTCPNoReply tcpSocket.Client m_writer AMQPClient.Client.TuneOKStr
commandNoReply AMQPClient.Client.TuneOK
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.ConnectionOpenStr
command (AMQPClient.Client.ConnectionOpen @"/")
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.ChannelOpenStr
command (AMQPClient.Client.ChannelOpen)
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.QueueDeclareStr
command (AMQPClient.Client.QueueDeclare @"foocar")
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.QueueBindStr
command (AMQPClient.Client.QueueBind @"foocar" @"Test" "")
//sendTCPNoReply tcpSocket.Client m_writer AMQPClient.Client.BasicPublishStr
commandNoReply (AMQPClient.Client.BasicPublish @"Test" "")
//sendTCPNoReply tcpSocket.Client m_writer AMQPClient.Client.ContentHeaderStr
commandNoReply (AMQPClient.Client.ContentHeader (int64 10))
sendTCPNoReply tcpSocket.Client m_writer AMQPClient.Client.ContentBodyStr
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.ChannelCloseStr
command (AMQPClient.Client.ChannelClose)
//sendTCPCommand tcpSocket.Client m_writer AMQPClient.Client.ConnectionCloseStr
command (AMQPClient.Client.ConnectionClose)
let SOCKET_CLOSING_TIMEOUT = 2
tcpSocket.LingerState <- new LingerOption(true, SOCKET_CLOSING_TIMEOUT)
tcpSocket.Close()

let ms5 = new MemoryStream(1024)
let bw5 = new BinaryWriter(ms5)
let o5 = AMQPClient.Client.ConnectionOpen @"/"
AMQPClient.Client.MethodFrame0 05 bw5

let buffer5 = ms5.ToArray()

let socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
let hostName = "rabbit5.np.wc1.yellowpages.com"
let ipHostEntry = Dns.GetHostEntry(hostName)

printfn "Server address: %s" (ipHostEntry.AddressList.[0].ToString())
socket.Connect(hostName,port)
printfn "Socket connected to: %s" (socket.RemoteEndPoint.ToString() )
sendCommand socket AMQPClient.Client.ProtocolHeader
sendCommand socket AMQPClient.Client.StartOK
sendCommand socket AMQPClient.Client.TuneOK
sendCommand socket AMQPClient.Client.ConnectionOpenStr
sendCommand socket AMQPClient.Client.ChannelOpen

let ms1 = new MemoryStream(1024)
let bw1 = new BinaryWriter(ms1)

// start-ok
write32 bw1 0x01000000
write32 bw1 0x00002400
write32 bw1 0x0a000b00
write32 bw1 0x00000005
write32 bw1 0x504c4149
write32 bw1 0x4e000000
write32 bw1 0x0c006775
write32 bw1 0x65737400
write32 bw1 0x67756573
write32 bw1 0x7405656e
write32 bw1 0x5f5553ce

printfn "Sending data..."
let buffer4 = ms1.ToArray()

#load "Pickle.fs"
open AMQPClient
open AMQPClient.CorePickle

let ms3 = new MemoryStream(1024)
let bw3 = new BinaryWriter(ms3)

let out3 = writeSeq bw3 Client.StartOK
let a3 = ms3.ToArray()
let s3 = ByteToHex a3

socket.Send( buffer4 )
let buffer5 = Array.zeroCreate<byte> 1024
let len5 = socket.Receive(buffer5)
printfn "Socket received: %d bytes" (len)


let read160 (reader:BinaryReader) =
    let a16 = reader.ReadBytes(2)
    let v = Array.rev a16
    let i16 = System.BitConverter.ToUInt16(v,0)
    i16

let read8 (reader:BinaryReader) =
    reader.ReadByte()

let read16 (reader:BinaryReader) =
    IPAddress.NetworkToHostOrder(reader.ReadInt16())

let read32 (reader:BinaryReader) =
    IPAddress.NetworkToHostOrder(reader.ReadInt32())

let readGeneralFrame (reader:BinaryReader) =
    let frameType = read8 reader
    let channel = read16 reader
    let sizeFrame = read32 reader
    (frameType, channel, sizeFrame)

let readShortString (reader:BinaryReader) =
    let len = read8 reader
    new string(reader.ReadChars(int len))

let readLongString (reader:BinaryReader) =
    let len = read32 reader
    new string(reader.ReadChars(int len))

let readFieldName (reader:BinaryReader) =
    let len = read8 reader
    reader.ReadChars(int len)

let readFieldTable (reader:BinaryReader) =
    let len = read32 reader
    len
    
let readFieldValueType (reader:BinaryReader) =
    read8 reader

let readFieldValue (reader:BinaryReader) =
    let enum = char(read8 reader)
    match enum with
    | 't' -> reader.ReadBytes(1)
    | 'b' -> reader.ReadBytes(1)
    | 'B' -> reader.ReadBytes(1)
    | 'U' -> reader.ReadBytes(2)
    | 'u' -> reader.ReadBytes(2)
    | 'I' -> reader.ReadBytes(4)
    | 'i' -> reader.ReadBytes(4)
    | 'L' -> reader.ReadBytes(8)
    | 'l' -> reader.ReadBytes(8)
    | 'f' -> reader.ReadBytes(4)
    | 'd' -> reader.ReadBytes(8)
    | 'D' -> reader.ReadBytes(0)
    | 's' -> Encoding.ASCII.GetBytes (readShortString reader)
    | 'S' -> Encoding.ASCII.GetBytes (readLongString reader)
    | 'A' -> reader.ReadBytes(0)
    | 'T' -> reader.ReadBytes(0)
    | 'F' -> reader.ReadBytes(0)
    | 'V' -> reader.ReadBytes(0)
    | _ -> reader.ReadBytes(0)

let iterField (reader:BinaryReader) =
    let mutable name = [|'C'|]
    let mutable b = [| byte 0 |]
    while name.Length > 0 do
        name <- (readFieldName reader)
        let mutable nameString = new string(name)
        printfn "%s" nameString
        b <- (readFieldValue reader)
    0

let ms2 = new MemoryStream(buffer2)
let ow = new BinaryReader(ms2)
let (frameType,channel,sizeFrame) = readGeneralFrame ow
let classID = read16 ow
let methodID = read16 ow
let versionMajor = read8 ow
let versionMinor = read8 ow
let fieldLen = read32 ow
let field1 = readFieldName ow
let field2 = read8 ow
let field3 = read32 ow
iterField ow
let field4 = readFieldName ow
let field5 = readFieldValue ow
let field6 = readFieldName ow
let field7 = readFieldValue ow
let field8 = readFieldName ow
let field9 = readFieldValue ow
let field10 = readFieldName ow
let field11 = readFieldValue ow
let field12 = readFieldName ow
let field13 = readFieldValue ow


let next8 (reader:BinaryReader) =
    let value = reader.ReadByte()
    printf "0x%x %c " (value) (char value)

next8 ow
