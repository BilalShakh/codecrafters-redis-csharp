namespace codecrafters_redis.src.Data
{
    public class XRangeOutput
    {
        public required string Id { get; set; }
        public required string[] Fields { get; set; }

        public override bool Equals(object? obj)
        {
            return Equals(obj as XRangeOutput);
        }

        public bool Equals(XRangeOutput? other)
        {
            if (other is null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return Id == other.Id && Fields.SequenceEqual(other.Fields);
        }

        public override int GetHashCode()
        {
            int hash = 17;
            hash = hash * 31 + Id.GetHashCode();
            hash = hash * 31 + Fields.Aggregate(0, (acc, field) => acc + field.GetHashCode());
            return hash;
        }
    }
}
