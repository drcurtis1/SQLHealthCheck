using System;
using System.Collections.Generic;
using System.Text;

namespace SQLHealthCheck.Common
{
    public class FileHeaders
    {
        public List<QueryDetails> queryDetails = new List<QueryDetails>();
        public struct QueryDetails
        {
            public string ColumnHeader { get; set; }

            public string QueryFileName { get; set; }

            public string QueryOutputFile { get; set; }

            public string IsStoredProcedure { get; set; }

            public string ExecStoredProcedure { get; set; }
        }

        public FileHeaders()
        {
            var files = System.IO.Directory.GetFiles(@".\FileDetails\");

            foreach(var a in files)
            {
                var result = new QueryDetails();

                var data = System.IO.File.ReadAllLines(a);

                result.ColumnHeader = data[0];
                result.QueryFileName = data[1];
                result.QueryOutputFile = data[2];
                result.IsStoredProcedure = data[3];
                result.ExecStoredProcedure = data[4];

                queryDetails.Add(result);
            }
        }

    }
}
