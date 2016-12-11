namespace AMQP

module Frames =

    //
    // AMQP client protocol
    //
    // AMQP Protocol documentation:
    // http://www.amqp.org/resources/download
    //
    
    open System.IO
    open AMQP.CorePickle

    let CH_01 = 1
    let FRAME_MAX = 0x20000

    type FrameID =
        | Method = 1
        | Header = 2
        | Body = 3
        | HeartBeat = 8
        | End = 0xce

    type ClassID = 
        | Connection  = 10
        | Channel = 20
        | Exchange = 40
        | Queue = 50
        | Basic = 60
        | Tx = 90

    let p_methodType bw =
        p_byte (FrameID.Method.GetHashCode()) bw

    let p_headerType bw =
        p_byte (FrameID.Header.GetHashCode()) bw

    let p_bodyType bw =
        p_byte (FrameID.Body.GetHashCode()) bw

    let p_frameEnd bw =
        p_byte (FrameID.End.GetHashCode()) bw

    let p_reserved1 bw =
        p_short 0 bw

    //
    // Frame Header section
    //
    // AMQP begins each command with a "Header Frame"
    // There are several types of headers, including method, content and body.
    // For AMQP Protocol documentation see: http://www.amqp.org/resources/download
    //

    let ProtocolFrame (payload:byte[]) (writer:BinaryWriter) =
        // Define protocol version: 'AMQP0091'
        p_char 'A' writer
        p_char 'M' writer
        p_char 'Q' writer
        p_char 'P' writer
        p_byte 0 writer
        p_byte 0 writer
        p_byte 9 writer
        p_byte 1 writer

    let MethodFrame0 (payload:byte[]) (writer:BinaryWriter) =
        p_methodType writer // Frame type 1
        p_short 0 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end
 
    let MethodFrame1 (payload:byte[]) (writer:BinaryWriter) =
        p_methodType writer // Frame type 1
        p_short CH_01 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end
 
    let HeaderFrame (payload:byte[]) (writer:BinaryWriter) =
        p_headerType writer // Frame type 2
        p_short CH_01 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end

    let BodyFrame (payload:byte[]) (writer:BinaryWriter) =
        p_byte (FrameID.Body.GetHashCode()) writer // Frame type 3
        p_short CH_01 writer
        p_bytes payload writer
        p_frameEnd writer // Frame end

    let ProtocolHeader:(BinaryWriter -> unit) =
        let ProtocolHeaderBinary =
            let ms = new MemoryStream(8)
            use bw = new BinaryWriter(ms)
            ms.ToArray()
        let cmd = ProtocolHeaderBinary
        let result = ProtocolFrame cmd
        result
        
    let ContentHeader contentSize =
        let ContentHeaderBinary contentSize =
            let ms = new MemoryStream(256)
            use bw = new BinaryWriter(ms)
            p_short (ClassID.Basic.GetHashCode()) bw // ClassID - must match method frame class id
            p_short 0 bw // weight field - unused, must be zero
            p_longlong contentSize bw // total size of the content body, i.e. sum of body sizes for content body frames
            p_short 0x9000 bw // Property flags
            p_shortString @"text/plain" bw
            p_byte 0x2 bw // Delivery mode bit
            ms.ToArray()
        let cmd = ContentHeaderBinary contentSize
        HeaderFrame cmd

    let ContentBody (payload:byte[]) =
        BodyFrame payload

