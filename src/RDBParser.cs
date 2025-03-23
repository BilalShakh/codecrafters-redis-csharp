namespace codecrafters_redis.src
{
    class RDBParser
    {
        private static string RDBFileDirectory = string.Empty;
        private static string RDBFileName = string.Empty;

        public RDBParser(string rdbFileName, string rdbFileDirectory)
        {
            RDBFileName = rdbFileName;
            RDBFileDirectory = rdbFileDirectory;
            LoadContents();
        }

        static void LoadContents()
        {
            string filePath = Path.Combine(RDBFileDirectory, RDBFileName);
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File {filePath} does not exist!");
                return;
            }
            try
            {
                byte[] data = File.ReadAllBytes(filePath);
                Console.WriteLine(
                    $"File read successfully. Data (hex): {BitConverter.ToString(data)}");
                ParseRedisRdbData(data);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"An error occurred while loading contents: {ex.Message}");
            }
        }

        static void ParseRedisRdbData(byte[] data)
        {
            int index = 0;
            try
            {
                while (index < data.Length)
                {
                    if (data[index] == 0xFB) // Start of database section
                    {
                        index = ParseDatabaseSection(data, index);
                        if (data[index] == 0xFF)
                        {
                            Console.WriteLine("End of database section detected.");
                            break;
                        }
                    }
                    else
                    {
                        index++; // Skip unknown or unhandled sections
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing RDB data: {ex.Message}");
                throw;
            }
        }

        static int ParseDatabaseSection(byte[] data, int startIndex)
        {
            int index = startIndex + 1;
            int length = data[index] + data[index + 1];
            Console.WriteLine(
                $"Database section detected. Key-value count: {length}");
            index += 2;
            for (int i = 0; i < length; i++)
            {
                ulong expiryTimeStampFC = 0;
                uint expiryTimeStampFD = 0;
                switch (data[index])
                {
                    case 0xFC:
                        index++;
                        expiryTimeStampFC = Utilities.ExtractUInt64(data, ref index);
                        Console.WriteLine($"Extracted expiry information. Milliseconds information. Timestamp: {expiryTimeStampFC}");
                        index++; // Skip the 0x00 byte
                        break;
                    case 0xFD:
                        index++;
                        expiryTimeStampFD = Utilities.ExtractUInt32(data, ref index);
                        Console.WriteLine($"Extracted expiry information. Seconds information. Timestamp: {expiryTimeStampFD}");
                        index++; // Skip the 0x00 byte
                        break;
                    case 0x00:
                        index++;
                        Console.WriteLine("Skipping 0x00 byte.");
                        break;
                    case 0xFF:
                        Console.WriteLine("End of database section detected.");
                        return index;
                }

                // Parse key
                int keyLength = data[index];
                Console.WriteLine($"Key length: {keyLength}");
                index++;
                string key = Utilities.ParseString(data, ref index, keyLength);
                Console.WriteLine($"Parsed key: {key}");

                // Parse value
                int valueLength = data[index];
                Console.WriteLine($"Value length: {valueLength}");
                index++;
                string value = Utilities.ParseString(data, ref index, valueLength);
                Console.WriteLine($"Parsed value: {value}");

                if (key.Length == 0)
                {
                    Console.WriteLine("Empty key found. Skipping.");
                    continue;
                }
                if (MasterClient.dataStore.ContainsKey(key))
                {
                    MasterClient.dataStore[key] = value;
                    Console.WriteLine($"Key-Value pair updated: {key} => {value}");
                    continue;
                }
                MasterClient.dataStore.Add(key, value);
                Console.WriteLine($"Key-Value pair added: {key} => {value}");

                if (expiryTimeStampFC != 0)
                {
                    _ = HandleTimeStampExpiry((long)expiryTimeStampFC, key, false);
                    expiryTimeStampFC = 0;
                }
                else if (expiryTimeStampFD != 0)
                {
                    _ = HandleTimeStampExpiry(expiryTimeStampFD, key, true);
                    expiryTimeStampFD = 0;
                }
            }
            return index;
        }

        static async Task HandleTimeStampExpiry(long unixTimeStamp, string key, bool isSeconds)
        {
            long currentUnixTime = isSeconds ? DateTimeOffset.UtcNow.ToUnixTimeSeconds() : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            long delay = unixTimeStamp - currentUnixTime;
            Console.WriteLine($"key: {key} Delay: {delay} unixTimeStamp: {unixTimeStamp} Now: {currentUnixTime}");
            if (delay <= 0)
            {
                Console.WriteLine($"Expiry time has already passed. Removing key. Done: {MasterClient.dataStore.Remove(key)}");
                return;
            }
            await Task.Delay((int)delay);
            MasterClient.dataStore.Remove(key);
        }
    }
}
