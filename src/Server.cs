namespace codecrafters_redis.src;

// Uncomment this block to pass the first stage
public class Server
{
    private static string RDBFileDirectory = string.Empty;
    private static string RDBFileName = string.Empty;
    private static string MasterHost = string.Empty;
    private static int MasterPort = 0;
    private static MasterClient? masterClient;
    private static RDBParser? rdbParser;
    private static SlaveClient? slaveClient;

    public static void Main(string[] args)
    {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        Console.WriteLine("Logs from your program will appear here!");

        LoadUpArgs(args);
        rdbParser = new(RDBFileName, RDBFileDirectory);
        slaveClient = new(MasterHost, MasterPort);
        masterClient = new(MasterHost, MasterPort, RDBFileName, RDBFileDirectory);
    }

    static void LoadUpArgs(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--dir":
                    if (i + 1 < args.Length)
                        RDBFileDirectory = args[i + 1];
                    break;
                case "--dbfilename":
                    if (i + 1 < args.Length)
                        RDBFileName = args[i + 1];
                    break;
                case "--port":
                    if (i + 1 < args.Length)
                        port = int.Parse(args[i + 1]);
                    break;
                case "--replicaof":
                    if (i + 1 < args.Length)
                    {
                        string[] argumentParts = args[i + 1].Split(' ');
                        MasterHost = argumentParts[0];
                        MasterPort = int.Parse(argumentParts[1]);
                    }
                    break;
            }
        }
    }
}
