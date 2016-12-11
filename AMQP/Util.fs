namespace AMQP

module Util =

    open System

    // Sequence Random Permutation
    // A generic function that randomly permutes the elements of a sequence
    let Scramble (sqn : seq<'T>) = 
        let rnd = new Random()
        let rec scramble2 (sqn : seq<'T>) = 
            /// Removes an element from a sequence.
            let remove n sqn = sqn |> Seq.filter (fun x -> x <> n)
 
            seq {
                let x = sqn |> Seq.nth (rnd.Next(0, sqn |> Seq.length))
                yield x
                let sqn' = remove x sqn
                if not (sqn' |> Seq.isEmpty) then
                    yield! scramble2 sqn'
            }
        scramble2 sqn
    // Example usage:
    //let test = scramble ['1' .. '9'] |> Seq.toList

    // Functional version of Dns.GetHostAddresses
    let GetHostAddresses hosts =

        let GetHostAddresses0 list =
            try
                Some(System.Net.Dns.GetHostAddresses( list ))
            with
            |_  -> None

        seq { for host in hosts do
                let ipList = GetHostAddresses0( host )
                match ipList with
                | Some(result) -> yield! result
                | None -> () }

    // Given a comma delmited list of host names, return a single IP address.
    // Use a random round robin algorithm.
    let RoundRobinHost (strList:string) =
        strList.Split [|',';' ';'\t'|]
        |> Seq.ofArray
        |> GetHostAddresses
        |> Scramble
        |> Seq.head  



