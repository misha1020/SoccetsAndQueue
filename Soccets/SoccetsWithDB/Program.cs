using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data.SQLite;
using System.Data;
using System.IO;
using System.Data.SqlClient;
using System.Runtime.Serialization.Formatters.Binary;
using RabbitMQ.Client;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using RabbitMQ.Client.Events;

namespace SoccetsWithDB
{
    class Program
    {
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
            SoccetsSend();
            Console.WriteLine("Сокеты отправлены");

            string dbFileName = "StudentsDB.db";
            DataSet dSet = ReadDataFromFile(dbFileName);
            if (dSet != null && dSet.Tables.Count > 0)
            {
                DataSet finalSet = new DataSet("Final");
                finalSet = DataNormalize(dSet);
                //QueueSend(finalSet);
                //Print(finalSet);
            }
            else
                Console.Write("Датасэт пуст");

            Console.ReadKey();
        }


        public static void SoccetsSend()
        {
            int port = 11000;

            IPHostEntry ipHost = Dns.GetHostEntry("localhost");
            IPAddress ipAddr = ipHost.AddressList[0];
            IPEndPoint ipEndPoint = new IPEndPoint(ipAddr, port);
            Socket sListener = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                sListener.Bind(ipEndPoint);
                sListener.Listen(10);
                while (true)
                {
                    Console.WriteLine("Ожидаем соединение через порт {0}", ipEndPoint);

                    Socket handler = sListener.Accept();
                    string data = null;


                    byte[] bytes = new byte[10240];
                    int bytesRec = handler.Receive(bytes);
                    DataSet dSet = BytesToDataSet(bytes);
                    DataSet finalSet = new DataSet("Final");
                    finalSet = DataNormalize(dSet);

                    data += Encoding.UTF8.GetString(bytes, 0, bytesRec);



                    handler.Send(DataSetToBytes(finalSet));

                    if (data.IndexOf("<TheEnd>") > -1)
                    {
                        Console.WriteLine("Сервер завершил соединение с клиентом.");
                        break;
                    }

                    handler.Shutdown(SocketShutdown.Both);
                    handler.Close();
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

        public static void QueueSend(DataSet finalSet)
        {
            Console.WriteLine("Нажмите клавишу [enter], чтобы отправить данные");

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                    string input = "";
                    do
                    {
                        input = Console.ReadLine();
                        var body = DataSetToBytes(finalSet);
                        channel.BasicPublish(exchange: "logs", routingKey: "", basicProperties: null, body: body);
                        Console.WriteLine("Данные отправлены");
                    }
                    while (input.ToUpper() != "Q");
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
            DataColumn idSpecialty = new DataColumn("idSpecialty", Type.GetType("System.Int32"));
            tStudent.Columns.AddRange(new DataColumn[] { idStudentCol, nameStudentCol, surnameStudentCol, idSpecialty });
            tStudent.PrimaryKey = new DataColumn[] { tStudent.Columns["Id"] };

            DataTable tSpecialty = new DataTable("tSpecialty");
            DataColumn idSpecialtyCol = ColumnIdInit(new DataColumn("IdSpecialty", Type.GetType("System.Int32")));
            DataColumn specialtyCol = new DataColumn("Specialty", Type.GetType("System.String"));
            DataColumn idFaculty = new DataColumn("idFaculty", Type.GetType("System.Int32"));
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
            DataColumn idCity = new DataColumn("idCity", Type.GetType("System.Int32"));
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

        static DataSet BytesToDataSet(byte[] byteArrayData)
        {
            DataSet ds;
            using (MemoryStream stream = new MemoryStream(byteArrayData))
            {
                BinaryFormatter bformatter = new BinaryFormatter();
                ds = (DataSet)bformatter.Deserialize(stream);
            }
            return ds;
        }

        static byte[] DataSetToBytes(DataSet dataSet)
        {
            MemoryStream stream = new System.IO.MemoryStream();
            System.Runtime.Serialization.IFormatter formatter = new BinaryFormatter();
            formatter.Serialize(stream, dataSet);
            byte[] bytes = stream.GetBuffer();
            return bytes;
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
    }
}
