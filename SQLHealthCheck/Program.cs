using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using SQLHealthCheck.Common;
using static SQLHealthCheck.Common.FileHeaders;

namespace SQLHealthCheck
{

    class Program
    {
        private static string _menu = "1 : Run a healthcheck on a single instance. \n2 : Run a healthcheck on multiple instances. \n ";
        private static List<string> _connectionStrings = new List<string>();
        private static string _tempConnection = "Server=localhost;Database=master;User Id=sa;Password=Test123;";
        private static string _baseResultPath = @".\Results\";
        private static string _baseScriptPath = @".\Scripts\";
        static void Main(string[] args)
        {
            var setup = new FileHeaders();
            // Menu();
            
            foreach(var a in setup.queryDetails)
            {
                ExecuteSqlCommand(_tempConnection, "localhost",a);
            }
            
            Console.Read();
        }

        public static void Menu()
        {
            Console.WriteLine(_menu);
            var a = Console.ReadLine();

            switch (a)
            {
                case "1":
                    throw new NotImplementedException();
                case "2":
                    throw new NotImplementedException();
                default:
                    Menu();
                    break;
            }
        }

        static bool ExecuteSqlCheck(string instanceName, string username, string password, int port)
        {

            throw new NotImplementedException();
        }

        static void ExecuteSqlCommand(string connectionString, string instanceName, QueryDetails queryDetails)
        {
            string outputFile = _baseResultPath + instanceName + "_" + queryDetails.QueryOutputFile;
            
            var sqlClient = new SqlConnection();

            var sqlCommand = ReadSqlCommand(_baseScriptPath + queryDetails.QueryFileName);

            using (SqlConnection con = new SqlConnection(
                connectionString))
            {
                var result = new List<string>();
                result.Add(queryDetails.ColumnHeader);

                con.Open();
                try
                {


                    // Issue a SQL Command to the server.
                    using (SqlCommand command = new SqlCommand(
                        sqlCommand, con))
                    {
                        var reader = command.ExecuteReader();
                        var columns = reader.FieldCount;


                        while(reader.Read())
                        {
                            var sb = new StringBuilder();
                            
                            for (int i = 0; i < columns; i++)
                            {
                                switch (reader.GetFieldType(i).ToString())
                                {
                                    case "System.Int32":
                                        sb.Append(reader.GetInt32(i));
                                        break;
                                    case "System.String":
                                        sb.Append(reader.GetString(i));
                                        break;
                                    case "System.DateTime":
                                        sb.Append(reader.GetDateTime(i));
                                        break;
                                    case "System.Single":
                                        sb.Append(reader.GetFloat(i));
                                        break;
                                    case "System.Boolean":
                                        sb.Append(reader.GetBoolean(i));
                                        break;
                                    case "System.Double":
                                        sb.Append(reader.GetDouble(i));
                                        break;
                                    case "System.Int64":
                                        sb.Append(reader.GetInt64(i));
                                        break;
                                    case "System.Decimal":
                                        sb.Append(reader.GetDecimal(i));
                                        break;
                                }

                                if (i < columns - 1) { sb.Append(','); }
                            }
                            result.Add(sb.ToString());
                        }
                    }
                }
                catch(Exception ex)
                {
                    throw ex;
                }

                WriteSqlCommandOutput(outputFile, result);
            }
            
        }

        private static string ReadSqlCommand(string filename)
        {
            return File.ReadAllText(filename);
        }

        private static void WriteSqlCommandOutput(string fileName, List<string> content)
        {
            try
            {
                var fileResult = File.OpenRead(fileName);
                fileResult.Close();
            }
            catch(DirectoryNotFoundException)
            {
                Directory.CreateDirectory(_baseResultPath);
            }
            catch (FileNotFoundException)
            {
                var fileResult = File.Create(fileName);
               // File.WriteAllLines(fileName)
                fileResult.Close();
            }

            File.WriteAllLines(fileName, content);
        }
    }
}