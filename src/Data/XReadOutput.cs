namespace codecrafters_redis.src.Data
{
    public class XReadOutput
    {
        public required string StreamName { get; set; }
        public required List<XRangeOutput> Outputs { get; set; }

        public override bool Equals(object? obj)
        {
            return Equals(obj as XReadOutput);
        }

        public bool Equals(XReadOutput? other)
        {
            if (other is null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return StreamName == other.StreamName && Outputs.SequenceEqual(other.Outputs);
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 31 + StreamName.GetHashCode();
            hash = hash * 31 + Outputs.Aggregate(0, (acc, output) => acc + output.GetHashCode());
            return hash;
        }
    }
}
