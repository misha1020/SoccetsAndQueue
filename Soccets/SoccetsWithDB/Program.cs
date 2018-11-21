﻿using System;
using System.Text;
using System.Data.SQLite;
using System.Data;
using System.IO;
using System.Data.SqlClient;
using System.Runtime.Serialization.Formatters.Binary;
using RabbitMQ.Client;
using System.Net;
using System.Net.Sockets;
using RabbitMQ.Client.Events;

namespace SoccetsWithDB
{
    class Program
    {
        static string sConnStr = new SqlConnectionStringBuilder()
        {
            DataSource = "E493",
            InitialCatalog = "Stud",
            IntegratedSecurity = true
        }.ConnectionString;

        public static DataColumn ColumnIdInit(DataColumn idColumn)
        {
            idColumn.Unique = true;
            idColumn.AllowDBNull = false;
            idColumn.AutoIncrement = true;
            idColumn.AutoIncrementSeed = 1;
            idColumn.AutoIncrementStep = 1;
            return idColumn;
        }

        static void Main(string[] args)
        {
            while (true)
            {
                SoccetsRecieve();
                //QueueRecieve();
            }
        }

        public static void SoccetsRecieve()
        {
            int port = 11000;

            try
            {
                Socket reciever = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                reciever.Bind(new IPEndPoint(IPAddress.Any, port));
                reciever.Listen(10);

                DataSet dSet = new DataSet();
                while (true)
                {
                    Console.WriteLine("Ожидаем соединение через порт {0}");
                    Socket handler = reciever.Accept();

                    byte[] length = new byte[256];
                    handler.Receive(length, 0, length.Length, SocketFlags.None);
                    int bytesRec = FromBytes<int>(length);
                    byte[] bytes = new byte[bytesRec];

                    int a = 0;
                    int step = bytesRec;
                    while (a < bytesRec)
                    {
                        if (a + step > bytesRec)
                            step = bytesRec - a;
                        a += handler.Receive(bytes, a, step, SocketFlags.None);
                    }

                    dSet = FromBytes<DataSet>(bytes);
                    handler.Shutdown(SocketShutdown.Both);
                    handler.Close();
                    WriteToDBAndPrint(dSet);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {                
                Console.ReadLine();
            }
        }

        public static void QueueRecieve()
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout");
                    var queueName = channel.QueueDeclare().QueueName;
                    channel.QueueBind(queue: queueName,
                                      exchange: "logs",
                                      routingKey: "");

                    Console.WriteLine("Ожидание...");

                    var consumer = new EventingBasicConsumer(channel);
                    DataSet dSet = new DataSet();
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        dSet = FromBytes<DataSet>(body);
                    };
                    channel.BasicConsume(queue: queueName,
                                    autoAck: true,
                                    consumer: consumer);

                    Console.WriteLine("Нажмите клавишу [enter], чтобы считать данные из очереди");
                    Console.ReadLine();
                    WriteToDBAndPrint(dSet);
                }
            }
        }

        public static DataSet ReadDataFromFile(string dbFileName)
        {
            SQLiteConnection m_dbConn;
            SQLiteCommand m_sqlCmd;

            m_dbConn = new SQLiteConnection();
            m_sqlCmd = new SQLiteCommand();

            String sqlQuery;
            DataSet dSet = new DataSet();

            m_sqlCmd.Connection = m_dbConn;
            m_dbConn = new SQLiteConnection("Data Source=" + dbFileName + ";Version=3;");
            m_dbConn.Open();
            try
            {
                if (m_dbConn.State != ConnectionState.Open)
                {
                    Console.WriteLine("Откройте соединение с базой данных");
                }
                else
                {
                    sqlQuery = "SELECT * FROM Students";
                    SQLiteDataAdapter adapter = new SQLiteDataAdapter(sqlQuery, m_dbConn);
                    adapter.Fill(dSet);
                }
            }
            catch (SQLiteException ex)
            {
                Console.WriteLine("Error: " + ex.Message);
                return null;
            }

            return dSet;
        }

        public static DataSet DataNormalize(DataSet dSet)
        {
            DataSet finalSet = new DataSet();

            DataTable tStudent = new DataTable("tStudent");
            DataColumn idStudentCol = ColumnIdInit(new DataColumn("Id", Type.GetType("System.Int32")));
            DataColumn nameStudentCol = new DataColumn("Name", Type.GetType("System.String"));
            DataColumn surnameStudentCol = new DataColumn("Surname", Type.GetType("System.String"));
            DataColumn idSpecialty = new DataColumn("IdSpecialty", Type.GetType("System.Int32"));
            tStudent.Columns.AddRange(new DataColumn[] { idStudentCol, nameStudentCol, surnameStudentCol, idSpecialty });
            tStudent.PrimaryKey = new DataColumn[] { tStudent.Columns["Id"] };

            DataTable tSpecialty = new DataTable("tSpecialty");
            DataColumn idSpecialtyCol = ColumnIdInit(new DataColumn("IdSpecialty", Type.GetType("System.Int32")));
            DataColumn specialtyCol = new DataColumn("Specialty", Type.GetType("System.String"));
            DataColumn idFaculty = new DataColumn("IdFaculty", Type.GetType("System.Int32"));
            tSpecialty.Columns.AddRange(new DataColumn[] { idSpecialtyCol, specialtyCol, idFaculty });
            tSpecialty.PrimaryKey = new DataColumn[] { tSpecialty.Columns["Specialty"] };


            DataTable tFaculty = new DataTable("tFaculty");
            DataColumn idFacultyCol = ColumnIdInit(new DataColumn("IdFaculty", Type.GetType("System.Int32")));
            DataColumn facultyCol = new DataColumn("Faculty", Type.GetType("System.String"));
            DataColumn idUniversity = new DataColumn("IdUniversity", Type.GetType("System.Int32"));
            tFaculty.Columns.AddRange(new DataColumn[] { idFacultyCol, facultyCol, idUniversity });
            tFaculty.PrimaryKey = new DataColumn[] { tFaculty.Columns["Faculty"] };

            DataTable tUniversity = new DataTable("tUniversity");
            DataColumn idUniversityCol = ColumnIdInit(new DataColumn("IdUniversity", Type.GetType("System.Int32")));
            DataColumn universityCol = new DataColumn("University", Type.GetType("System.String"));
            DataColumn idCity = new DataColumn("IdCity", Type.GetType("System.Int32"));
            tUniversity.Columns.AddRange(new DataColumn[] { idUniversityCol, universityCol, idCity });
            tUniversity.PrimaryKey = new DataColumn[] { tUniversity.Columns["University"] };

            DataTable tCity = new DataTable("tCity");
            DataColumn idCityCol = ColumnIdInit(new DataColumn("IdCity", Type.GetType("System.Int32")));
            DataColumn cityCol = new DataColumn("City", Type.GetType("System.String"));
            tCity.Columns.AddRange(new DataColumn[] { idCityCol, cityCol });
            tCity.PrimaryKey = new DataColumn[] { tCity.Columns["City"] };

            finalSet.Tables.AddRange(new DataTable[] { tStudent, tSpecialty, tFaculty, tUniversity, tCity });

            foreach (DataTable dTable in dSet.Tables)
                foreach (DataRow row in dTable.Rows)
                {
                    string city = row.ItemArray[5].ToString();
                    if (!tCity.Rows.Contains(city))
                        tCity.Rows.Add(new object[] { null, city });

                    int id = (int)tCity.Rows.Find(city)[0];
                    string university = row.ItemArray[4].ToString();
                    if (!tUniversity.Rows.Contains(university))
                        tUniversity.Rows.Add(new object[] { null, university, id });

                    id = (int)tUniversity.Rows.Find(university)[0];
                    string faculty = row.ItemArray[3].ToString();
                    if (!tFaculty.Rows.Contains(faculty))
                        tFaculty.Rows.Add(new object[] { null, faculty, id });

                    id = (int)tFaculty.Rows.Find(faculty)[0];
                    string specialty = row.ItemArray[2].ToString();
                    if (!tSpecialty.Rows.Contains(specialty))
                        tSpecialty.Rows.Add(new object[] { null, specialty, id });

                    id = (int)tSpecialty.Rows.Find(specialty)[0];
                    string surname = row.ItemArray[1].ToString();
                    string name = row.ItemArray[0].ToString();
                    tStudent.Rows.Add(new object[] { null, name, surname, id });
                }
            return finalSet;
        }

        static byte[] ToBytes<T>(T parameters)
        {
            MemoryStream stream = new System.IO.MemoryStream();
            System.Runtime.Serialization.IFormatter formatter = new BinaryFormatter();
            formatter.Serialize(stream, parameters);

            byte[] bytes = stream.GetBuffer();

            return bytes;
        }

        static T FromBytes<T>(byte[] byteArrayData)
        {
            T parameters;
            using (MemoryStream stream = new MemoryStream(byteArrayData))
            {
                BinaryFormatter bformatter = new BinaryFormatter();
                parameters = (T)bformatter.Deserialize(stream);
            }
            return parameters;
        }

        static void Print(DataSet finalSet)
        {
            foreach (DataTable dTable in finalSet.Tables)
            {
                Console.WriteLine(dTable.TableName);
                foreach (DataColumn col in dTable.Columns)
                    Console.Write("\t{0}", col.ColumnName);
                Console.WriteLine();
                foreach (DataRow row in dTable.Rows)
                {
                    var cells = row.ItemArray;
                    foreach (object cell in cells)
                        Console.Write("\t{0}", cell);
                    Console.WriteLine();
                }
            }
        }

        private static void WriteToDBAndPrint(DataSet dSet)
        {
            DataSet dataFromClient = dSet;
            if (dataFromClient != null && dataFromClient.Tables.Count > 0)
            {
                DataSet finalSet = new DataSet("Final");
                finalSet = DataNormalize(dataFromClient);
                Print(finalSet);

                AddInfoToDB(finalSet);
            }
            else
                Console.WriteLine("Датасэт не был принят");
        }

        static void AddInfoToDB(DataSet finalSet)
        {
            using (var sConn = new SqlConnection(sConnStr))
            {
                sConn.Open();

                var sCommand = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"DELETE dbo.tStudent
                                    DELETE dbo.tSpecialty
                                    DELETE dbo.tFaculty
                                    DELETE dbo.tUniversity
                                    DELETE dbo.tCity"
                };
                sCommand.ExecuteNonQuery();
            }

            tableCityAdd(finalSet.Tables[4]);
            tableUnivercityAdd(finalSet.Tables[3]);
            tableFacultyAdd(finalSet.Tables[2]);
            tableSpecialtyAdd(finalSet.Tables[1]);
            tableStudentAdd(finalSet.Tables[0]);
        }

        static void tableCityAdd(DataTable tCity)
        {
            using (var sConn = new SqlConnection(sConnStr))
            {
                sConn.Open();
                for (int i = 0; i < tCity.Rows.Count; i++)
                {
                    var sCommand = new SqlCommand
                    {
                        Connection = sConn,
                        CommandText = @"INSERT INTO tCity(IdCity, City) 
                                    VALUES (@IdCity, @City)"
                    };
                    sCommand.Parameters.AddWithValue("@IdCity", tCity.Rows[i].ItemArray[0]);
                    sCommand.Parameters.AddWithValue("@City", tCity.Rows[i].ItemArray[1]);
                    sCommand.ExecuteNonQuery();
                }
            }
        }

        static void tableUnivercityAdd(DataTable tUniversity)
        {
            using (var sConn = new SqlConnection(sConnStr))
            {
                sConn.Open();
                for (int i = 0; i < tUniversity.Rows.Count; i++)
                {
                    var sCommand = new SqlCommand
                    {
                        Connection = sConn,
                        CommandText = @"INSERT INTO tUniversity(IdUniversity, University, IdCity) 
                                    VALUES (@IdUniversity, @University, @IdCity)"
                    };
                    sCommand.Parameters.AddWithValue("@IdUniversity", tUniversity.Rows[i].ItemArray[0]);
                    sCommand.Parameters.AddWithValue("@University", tUniversity.Rows[i].ItemArray[1]);
                    sCommand.Parameters.AddWithValue("@IdCity", tUniversity.Rows[i].ItemArray[2]);                    
                    sCommand.ExecuteNonQuery();
                }
            }
        }

        static void tableFacultyAdd(DataTable tFaculty)
        {
            using (var sConn = new SqlConnection(sConnStr))
            {
                sConn.Open();
                for (int i = 0; i < tFaculty.Rows.Count; i++)
                {
                    var sCommand = new SqlCommand
                    {
                        Connection = sConn,
                        CommandText = @"INSERT INTO tFaculty(IdFaculty, Faculty, IdUniversity) 
                                    VALUES (@IdFaculty, @Faculty, @IdUniversity)"
                    };
                    sCommand.Parameters.AddWithValue("@IdFaculty", tFaculty.Rows[i].ItemArray[0]);
                    sCommand.Parameters.AddWithValue("@Faculty", tFaculty.Rows[i].ItemArray[1]);
                    sCommand.Parameters.AddWithValue("@IdUniversity", tFaculty.Rows[i].ItemArray[2]);
                    sCommand.ExecuteNonQuery();
                }
            }
        }

        static void tableSpecialtyAdd(DataTable tSpecialty)
        {
            using (var sConn = new SqlConnection(sConnStr))
            {
                sConn.Open();
                for (int i = 0; i < tSpecialty.Rows.Count; i++)
                {
                    var sCommand = new SqlCommand
                    {
                        Connection = sConn,
                        CommandText = @"INSERT INTO tSpecialty(IdSpecialty, Specialty, IdFaculty) 
                                    VALUES (@IdSpecialty, @Specialty, @IdFaculty)"
                    };
                    sCommand.Parameters.AddWithValue("@IdSpecialty", tSpecialty.Rows[i].ItemArray[0]);
                    sCommand.Parameters.AddWithValue("@Specialty", tSpecialty.Rows[i].ItemArray[1]);
                    sCommand.Parameters.AddWithValue("@IdFaculty", tSpecialty.Rows[i].ItemArray[2]);
                    sCommand.ExecuteNonQuery();
                }
            }
        }

        static void tableStudentAdd(DataTable tStudent)
        {
            using (var sConn = new SqlConnection(sConnStr))
            {
                sConn.Open();
                for (int i = 0; i < tStudent.Rows.Count; i++)
                {
                    var sCommand = new SqlCommand
                    {
                        Connection = sConn,
                        CommandText = @"INSERT INTO tStudent(Id, Name, Surname, IdSpecialty) 
                                    VALUES (@Id, @Name, @Surname, @IdSpecialty)"
                    };
                    sCommand.Parameters.AddWithValue("@Id", tStudent.Rows[i].ItemArray[0]);
                    sCommand.Parameters.AddWithValue("@Name", tStudent.Rows[i].ItemArray[1]);
                    sCommand.Parameters.AddWithValue("@Surname", tStudent.Rows[i].ItemArray[2]);
                    sCommand.Parameters.AddWithValue("@IdSpecialty", tStudent.Rows[i].ItemArray[3]);
                    sCommand.ExecuteNonQuery();
                }
            }
        }

    }
}
