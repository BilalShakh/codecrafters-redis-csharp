using codecrafters_redis.src.Data;
using System.Text;

namespace codecrafters_redis.src
{
    public class Utilities
    {
        public static string BuildArrayString(string[] args, bool noEndline = false)
        {
            var answer = string.Format("*{0}\r\n", args.Length);
            for (int i = 0; i < args.Length; i++)
            {
                var item = args[i];
                answer += string.Format("${0}\r\n{1}", item.Length, item);
                answer += (noEndline && i == args.Length - 1) ? "" : "\r\n";
            }
            return answer;
        }

        public static string BuildNestedArrayString(XRangeOutput[] outputs)
        {
            var answer = string.Format("*{0}\r\n", outputs.Length);
            foreach (var output in outputs)
            {
                answer += string.Format("*2\r\n${0}\r\n{1}\r\n*{2}\r\n", output.Id.Length, output.Id, output.Fields.Length);
                foreach (var field in output.Fields)
                {
                    answer += string.Format("${0}\r\n{1}\r\n", field.Length, field);
                }
            }
            return answer;
        }

        public static string BuildXReadOutputArrayString(XReadOutput[] outputs)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("*{0}\r\n", outputs.Length);

            foreach (var output in outputs)
            {
                sb.Append("*2\r\n");
                sb.AppendFormat("${0}\r\n{1}\r\n", output.StreamName.Length, output.StreamName);
                sb.AppendFormat("*{0}\r\n", output.Outputs.Count);

                foreach (var rangeOutput in output.Outputs)
                {
                    sb.Append("*2\r\n");
                    sb.AppendFormat("${0}\r\n{1}\r\n", rangeOutput.Id.Length, rangeOutput.Id);
                    sb.AppendFormat("*{0}\r\n", rangeOutput.Fields.Length / 2);

                    for (int i = 0; i + 1 < rangeOutput.Fields.Length; i += 2)
                    {
                        string field = rangeOutput.Fields[i];
                        string value = rangeOutput.Fields[i + 1];
                        sb.AppendFormat("${0}\r\n{1}\r\n", field.Length, field);
                        sb.AppendFormat("${0}\r\n{1}\r\n", value.Length, value);
                    }
                }
            }

            return sb.ToString();
        }


        public static string BuildBulkString(string value)
        {
            return $"${value.Length}\r\n{value}\r\n";
        }

        public static string BuildIntegerString(int value)
        {
            return $":{value}\r\n";
        }

        public static string BuildErrorString(string message)
        {
            return $"-ERR {message}\r\n";
        }

        public static string Generate40CharacterGuid()
        {
            string guid = Guid.NewGuid().ToString("N"); // 32 characters
            string extraChars = Guid.NewGuid().ToString("N").Substring(0, 8); // 8 additional characters
            return guid + extraChars; // 40 characters in total
        }

        public static ulong ExtractUInt64(byte[] data, ref int index)
        {
            if (index + 8 >= data.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index out of range for extracting UInt64.");
            }
            ulong value = BitConverter.ToUInt64(data, index);
            index += 8;
            return value;
        }

        public static uint ExtractUInt32(byte[] data, ref int index)
        {
            if (index + 4 >= data.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index out of range for extracting UInt32.");
            }
            uint value = BitConverter.ToUInt32(data, index);
            index += 4;
            return value;
        }

        public static string ParseString(byte[] data, ref int index, int length)
        {
            string result =
                Encoding.Default.GetString(data.Skip(index).Take(length).ToArray());
            index += length;
            return result;
        }

        public static bool isEmptyRDB(byte[] data)
        {
            // The base64 string to search for
            const string base64String = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

            // Convert the base64 string to a byte array
            byte[] base64Bytes = Convert.FromBase64String(base64String);

            // Search for the base64 byte array in the data byte array
            for (int i = 0; i <= data.Length - base64Bytes.Length; i++)
            {
                bool found = true;
                for (int j = 0; j < base64Bytes.Length; j++)
                {
                    if (data[i + j] != base64Bytes[j])
                    {
                        found = false;
                        break;
                    }
                }
                if (found)
                {
                    return true;
                }
            }
            return false;
        }
    }
}
