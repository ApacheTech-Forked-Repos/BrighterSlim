using System.Data.Common;

namespace ApacheTech.Common.BrighterSlim
{
    public interface IAmATransactionConnectionProvider : IAmARelationalDbConnectionProvider, IAmABoxTransactionProvider<DbTransaction> { }
}
