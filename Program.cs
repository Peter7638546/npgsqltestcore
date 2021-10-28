using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace npgsqltestcore
{
    class Program
    {
        static void Main(string[] args)
        {
            TestSpeed(true);
        }


        /// <summary>
        /// Scenario demonstrating problem described at
        /// https://stackoverflow.com/questions/69668912/postgresql-npgsql-slows-down-until-client-restarted
        /// When <paramref name="recreateSchema"/> is true, loop iterations gradually 
        /// get slower and slower, while the postgres.exe process corresponding
        /// to the (pooled) connection consumes more and more memory.
        /// With this very simple DB schema (just one table) speed deterioration  
        /// becomes noticeable quite gradually (after 10,000 iterations, takes
        /// twice as long) but with a real-world schema, it kicks in much more
        /// quickly. 
        /// </summary>
        /// <param name="recreateSchema">If true, loop recreates drops and recreates table for
        /// each iteration, with effects described above. If false, table is truncated
        /// instead, then memory consumption and speed remain more stable.
        /// </param>
        /// <remarks>PostgreSQL 13.3 and this test app both running on Windows 10</remarks>
        static void TestSpeed(bool recreateSchema)
        {
            DropSchema();
            CreateSchema();
            var sw = new Stopwatch();
            var timings = new List<long>();

            int reportInterval = recreateSchema ? 10 : 100;

            for (int i = 0; i < 1000000; i++)
            {

                sw.Restart();
                WriteData();
                DeleteAll();
                if (recreateSchema)
                {
                    DropSchema();
                    CreateSchema();
                }
                sw.Stop();

                timings.Add(sw.ElapsedMilliseconds);

                if (i == 0 || i % reportInterval != 0)
                    continue;

                Console.WriteLine($"{i} Average time: {timings.Average()}ms");
                timings.Clear();
            }
        }

        static void DeleteAll()
        {
            using (var conn = GetConnection())
            {
                using (var cmd = new NpgsqlCommand("", conn))
                {
                    cmd.CommandText = "truncate mytable";
                    cmd.ExecuteNonQuery();
                }
            }

        }

        static void WriteData()
        {
            using (var conn = GetConnection())
            {
                using (var cmd = new NpgsqlCommand("myfn", conn))
                {
                    cmd.CommandType = System.Data.CommandType.StoredProcedure;
                    var par1 = cmd.Parameters.AddWithValue("par_1", "");
                    var par2 = cmd.Parameters.AddWithValue("par_2", 1L);
                    var par3 = cmd.Parameters.AddWithValue("par_3", 1);
                    for (int i = 0; i < 10; i++)
                    {
                        par1.Value = "test" + (char)((int)'a' + i);
                        par2.Value = i;
                        par3.Value = i;
                        cmd.ExecuteNonQuery();
                    }

                }
            }

        }


        static void DropSchema()
        {
            using (var conn = GetConnection())
            {
                using (var cmd = new NpgsqlCommand("", conn))
                {
                    cmd.CommandText = "drop function if exists myfn";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "drop table if exists mytable";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        static void CreateSchema()
        {
            using (var conn = GetConnection())
            {
                using (var cmd = new NpgsqlCommand("", conn))
                {
                    cmd.CommandText = @"create table mytable
(
id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
value1 text,
value2 bigint,
value3 int
)
";

                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"create or replace function myfn(in par_1 text, in par_2 bigint, in par_3 integer)
returns integer
as
$BODY$
BEGIN
	insert into mytable(value1, value2, value3) values (par_1, par_2, par_3);
	return 1;
END;
$BODY$
LANGUAGE plpgsql;";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        static NpgsqlConnection GetConnection()
        {
            var conn = new NpgsqlConnection("Server=localhost;Port=5432;User Id=testuser;Password=testpwd;Database=testdb;Enlist=false");
            conn.Open();
            return conn;
        }
    }
}
