using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using ModbusRTU_TCP.Model;

namespace ModbusRTU_TCP.DAL
{
    public class DataExportDAL : IDisposable
    {
        private const int MaxRecordCount = 1000;
        private const string DbDirectory = @"D:\SQLiteData";
        private const string DbFileName = "ModbusRTU_TCP.db";
        private readonly string _dbFilePath;
        private readonly string _connectionString;
        private readonly object _dbLock = new object();
        private bool _disposed = false;

        public DataExportDAL()
        {
            try
            {
                if (!Directory.Exists(DbDirectory))
                {
                    Directory.CreateDirectory(DbDirectory);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"无法创建数据库目录 {DbDirectory}：{ex.Message}", ex);
            }

            _dbFilePath = Path.Combine(DbDirectory, DbFileName);
            _connectionString = $"Data Source={_dbFilePath};Version=3;Pooling=true;Max Pool Size=10;";
            InitializeDatabase();
        }

        private void InitializeDatabase()
        {
            lock (_dbLock)
            {
                try
                {
                    if (!File.Exists(_dbFilePath))
                    {
                        SQLiteConnection.CreateFile(_dbFilePath);
                    }

                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string createSql = @"
                        CREATE TABLE IF NOT EXISTS ModbusDataRecords (
                            Id INTEGER PRIMARY KEY AUTOINCREMENT,
                            CollectTime TEXT NOT NULL,
                            Temperature REAL NOT NULL,
                            Humidity REAL NOT NULL,
                            Status TEXT NOT NULL
                        );";
                        using (SQLiteCommand cmd = new SQLiteCommand(createSql, conn))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"数据库初始化失败：{ex.Message}", ex);
                }
            }
        }

        public event Action<string> OnDbLog;

        private void LogDb(string msg)
        {
            if (OnDbLog != null)
            {
                OnDbLog(msg);
            }
        }

        public bool SaveRecordToSqlite(DataRecord record)
        {
            if (record == null)
            {
                LogDb("【数据库写入失败】记录为空");
                return false;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string insertSql = @"
                        INSERT INTO ModbusDataRecords (CollectTime, Temperature, Humidity, Status)
                        VALUES (@CollectTime, @Temperature, @Humidity, @Status);";

                        using (SQLiteCommand cmd = new SQLiteCommand(insertSql, conn))
                        {
                            cmd.Parameters.AddWithValue("@CollectTime", record.CollectTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            cmd.Parameters.AddWithValue("@Temperature", record.Temperature);
                            cmd.Parameters.AddWithValue("@Humidity", record.Humidity);
                            cmd.Parameters.AddWithValue("@Status", record.Status ?? "正常");
                            cmd.ExecuteNonQuery();
                            record.Id = conn.LastInsertRowId;
                        }
                    }
                }
                LogDb($"【数据库写入成功】ID：{record.Id}，温度：{record.Temperature:0.0}℃，湿度：{record.Humidity:0.0}%");
                return true;
            }
            catch (Exception ex)
            {
                LogDb($"【数据库写入失败】{ex.Message}");
                return false;
            }
        }

        // 批量写入多条记录（内存缓冲刷库专用）
        // 核心优化：N 条记录共用一个事务提交，SQLite 只刷一次盘，比逐条写入快几十倍
        public bool SaveRecordsBatch(List<DataRecord> records)
        {
            if (records == null || records.Count == 0)
            {
                return false;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string insertSql = @"
                        INSERT INTO ModbusDataRecords (CollectTime, Temperature, Humidity, Status)
                        VALUES (@CollectTime, @Temperature, @Humidity, @Status);";

                        using (SQLiteTransaction tran = conn.BeginTransaction())   // 开启事务
                        using (SQLiteCommand cmd = new SQLiteCommand(insertSql, conn))
                        {
                            foreach (DataRecord record in records)
                            {
                                cmd.Parameters.AddWithValue("@CollectTime", record.CollectTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                cmd.Parameters.AddWithValue("@Temperature", record.Temperature);
                                cmd.Parameters.AddWithValue("@Humidity", record.Humidity);
                                cmd.Parameters.AddWithValue("@Status", record.Status ?? "正常");
                                cmd.ExecuteNonQuery();
                                record.Id = conn.LastInsertRowId;   // 回填自增ID，保持与单条写入行为一致
                            }
                            tran.Commit();   // N 条一起提交落盘
                        }
                    }
                }
                LogDb($"【批量写入成功】共 {records.Count} 条记录");
                return true;
            }
            catch (Exception ex)
            {
                LogDb($"【批量写入失败】{ex.Message}");
                return false;
            }
        }

        // 死信降级：刷库重试超限的批次追加写入本地TXT文件，防止数据彻底丢失（人工兜底）
        // 文件位置与数据库同目录：D:\SQLiteData\DeadLetterRecords.txt
        public bool SaveToDeadLetter(List<DataRecord> records)
        {
            if (records == null || records.Count == 0)
            {
                return false;
            }

            try
            {
                if (!Directory.Exists(DbDirectory))
                {
                    Directory.CreateDirectory(DbDirectory);
                }
                string deadLetterPath = Path.Combine(DbDirectory, "DeadLetterRecords.txt");

                StringBuilder sb = new StringBuilder();
                sb.AppendLine($"# 死信批次 {DateTime.Now:yyyy-MM-dd HH:mm:ss} 共{records.Count}条（多次刷库失败，人工核对后可补录）");
                foreach (DataRecord record in records)
                {
                    sb.AppendLine($"{record.CollectTime:yyyy-MM-dd HH:mm:ss},{record.Temperature:0.0},{record.Humidity:0.0},{record.Status}");
                }

                File.AppendAllText(deadLetterPath, sb.ToString(), Encoding.UTF8);   // 追加模式，不覆盖历史死信
                LogDb($"【死信落盘成功】{deadLetterPath}");
                return true;
            }
            catch (Exception ex)
            {
                LogDb($"【死信落盘失败】{ex.Message}");
                return false;
            }
        }

        public List<DataRecord> GetAllRecordsFromSqlite()
        {
            var result = new List<DataRecord>();
            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string querySql = "SELECT Id, CollectTime, Temperature, Humidity, Status FROM ModbusDataRecords ORDER BY CollectTime DESC;";
                        using (SQLiteCommand cmd = new SQLiteCommand(querySql, conn))
                        using (SQLiteDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                result.Add(MapRecord(reader));
                            }
                        }
                    }
                }
                LogDb($"【数据库查询成功】共 {result.Count} 条记录");
            }
            catch (Exception ex)
            {
                LogDb($"【数据库查询失败】{ex.Message}");
            }

            return result;
        }

        public DataRecord GetRecordById(long id)
        {
            if (id <= 0)
            {
                LogDb("【数据库查询失败】ID无效");
                return null;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string querySql = "SELECT Id, CollectTime, Temperature, Humidity, Status FROM ModbusDataRecords WHERE Id = @Id LIMIT 1;";
                        using (SQLiteCommand cmd = new SQLiteCommand(querySql, conn))
                        {
                            cmd.Parameters.AddWithValue("@Id", id);
                            using (SQLiteDataReader reader = cmd.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    return MapRecord(reader);
                                }
                            }
                        }
                    }
                }
                LogDb($"【数据库查询失败】未找到ID={id}的记录");
            }
            catch (Exception ex)
            {
                LogDb($"【数据库查询失败】ID={id}，{ex.Message}");
            }

            return null;
        }

        public List<DataRecord> QueryRecordsByTimeRange(DateTime startTime, DateTime endTime)
        {
            var result = new List<DataRecord>();
            if (startTime > endTime)
            {
                LogDb("【数据库查询失败】起始时间大于结束时间");
                return result;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string querySql = @"
                        SELECT Id, CollectTime, Temperature, Humidity, Status
                        FROM ModbusDataRecords
                        WHERE CollectTime >= @StartTime AND CollectTime <= @EndTime
                        ORDER BY CollectTime DESC;";

                        using (SQLiteCommand cmd = new SQLiteCommand(querySql, conn))
                        {
                            cmd.Parameters.AddWithValue("@StartTime", startTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            cmd.Parameters.AddWithValue("@EndTime", endTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            using (SQLiteDataReader reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    result.Add(MapRecord(reader));
                                }
                            }
                        }
                    }
                }
                LogDb($"【数据库时间范围查询成功】{startTime:yyyy-MM-dd}~{endTime:yyyy-MM-dd}，共 {result.Count} 条");
            }
            catch (Exception ex)
            {
                LogDb($"【数据库时间范围查询失败】{ex.Message}");
            }

            return result;
        }

        public bool UpdateRecord(DataRecord record)
        {
            if (record == null || record.Id <= 0)
            {
                LogDb("【数据库修改失败】记录无效");
                return false;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string updateSql = @"
                        UPDATE ModbusDataRecords
                        SET CollectTime = @CollectTime, Temperature = @Temperature, Humidity = @Humidity, Status = @Status
                        WHERE Id = @Id;";

                        using (SQLiteCommand cmd = new SQLiteCommand(updateSql, conn))
                        {
                            cmd.Parameters.AddWithValue("@CollectTime", record.CollectTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            cmd.Parameters.AddWithValue("@Temperature", record.Temperature);
                            cmd.Parameters.AddWithValue("@Humidity", record.Humidity);
                            cmd.Parameters.AddWithValue("@Status", record.Status ?? "正常");
                            cmd.Parameters.AddWithValue("@Id", record.Id);
                            int affected = cmd.ExecuteNonQuery();
                            if (affected > 0)
                            {
                                LogDb($"【数据库修改成功】ID：{record.Id}");
                                return true;
                            }
                            else
                            {
                                LogDb($"【数据库修改失败】未找到ID={record.Id}的记录");
                                return false;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDb($"【数据库修改失败】{ex.Message}");
                return false;
            }
        }

        public bool DeleteRecordById(long id)
        {
            if (id <= 0)
            {
                LogDb("【数据库删除失败】ID无效");
                return false;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string deleteSql = "DELETE FROM ModbusDataRecords WHERE Id = @Id;";
                        using (SQLiteCommand cmd = new SQLiteCommand(deleteSql, conn))
                        {
                            cmd.Parameters.AddWithValue("@Id", id);
                            int affected = cmd.ExecuteNonQuery();
                            if (affected > 0)
                            {
                                LogDb($"【数据库删除成功】ID：{id}");
                                return true;
                            }
                            else
                            {
                                LogDb($"【数据库删除失败】未找到ID={id}的记录");
                                return false;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDb($"【数据库删除失败】{ex.Message}");
                return false;
            }
        }

        public int DeleteRecordsByTimeRange(DateTime startTime, DateTime endTime)
        {
            if (startTime > endTime)
            {
                LogDb("【数据库删除失败】起始时间大于结束时间");
                return 0;
            }

            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string deleteSql = @"
                        DELETE FROM ModbusDataRecords
                        WHERE CollectTime >= @StartTime AND CollectTime <= @EndTime;";

                        using (SQLiteCommand cmd = new SQLiteCommand(deleteSql, conn))
                        {
                            cmd.Parameters.AddWithValue("@StartTime", startTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            cmd.Parameters.AddWithValue("@EndTime", endTime.ToString("yyyy-MM-dd HH:mm:ss"));
                            int affected = cmd.ExecuteNonQuery();
                            LogDb($"【数据库时间范围删除成功】删除 {affected} 条记录");
                            return affected;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDb($"【数据库时间范围删除失败】{ex.Message}");
                return 0;
            }
        }

        public int ClearAllRecords()
        {
            try
            {
                lock (_dbLock)
                {
                    using (SQLiteConnection conn = new SQLiteConnection(_connectionString))
                    {
                        conn.Open();
                        const string clearSql = "DELETE FROM ModbusDataRecords;";
                        using (SQLiteCommand cmd = new SQLiteCommand(clearSql, conn))
                        {
                            int affected = cmd.ExecuteNonQuery();
                            LogDb($"【数据库清空成功】删除 {affected} 条记录");
                            return affected;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogDb($"【数据库清空失败】{ex.Message}");
                return 0;
            }
        }

        private static DataRecord MapRecord(SQLiteDataReader reader)
        {
            DateTime collectTime;
            DateTime.TryParse(reader["CollectTime"].ToString(), out collectTime);

            return new DataRecord
            {
                Id = Convert.ToInt64(reader["Id"]),
                CollectTime = collectTime,
                Temperature = Convert.ToDouble(reader["Temperature"]),
                Humidity = Convert.ToDouble(reader["Humidity"]),
                Status = reader["Status"].ToString()
            };
        }

        public async Task<bool> ExportToTxtAsync(List<DataRecord> records, string filePath)
        {
            try
            {
                using (StreamWriter sw = new StreamWriter(filePath, false, Encoding.UTF8))
                {
                    await sw.WriteLineAsync("===== ModbusRTU_TCP 温湿度采集历史记录 =====");
                    await sw.WriteLineAsync($"导出时间：{DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                    await sw.WriteLineAsync($"记录总数：{records.Count}");
                    await sw.WriteLineAsync("==============================================");
                    
                    foreach (var record in records)
                    {
                        await sw.WriteLineAsync($"采集时间：{record.CollectTime:yyyy-MM-dd HH:mm:ss}");
                        await sw.WriteLineAsync($"温度：{record.Temperature:0.0} ℃");
                        await sw.WriteLineAsync($"湿度：{record.Humidity:0.0} %");
                        await sw.WriteLineAsync($"设备状态：{record.Status}");
                        await sw.WriteLineAsync("----------------------------------------------");
                    }
                }
                LogDb($"【导出TXT成功】路径：{filePath}，共 {records.Count} 条");
                return true;
            }
            catch (Exception ex)
            {
                LogDb($"【导出TXT失败】{ex.Message}");
                return false;
            }
        }

        public async Task<bool> ExportToCsvAsync(List<DataRecord> records, string filePath)
        {
            try
            {
                using (StreamWriter sw = new StreamWriter(filePath, false, Encoding.UTF8))
                {
                    await sw.WriteLineAsync("采集时间,温度(℃),湿度(%),设备状态");
                    
                    foreach (var record in records)
                    {
                        await sw.WriteLineAsync($"{record.CollectTime:yyyy-MM-dd HH:mm:ss},{record.Temperature:0.0},{record.Humidity:0.0},{record.Status}");
                    }
                }
                LogDb($"【导出CSV成功】路径：{filePath}，共 {records.Count} 条");
                return true;
            }
            catch (Exception ex)
            {
                LogDb($"【导出CSV失败】{ex.Message}");
                return false;
            }
        }

        public int GetMaxRecordCount()
        {
            return MaxRecordCount;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                SQLiteConnection.ClearAllPools();
            }
            _disposed = true;
        }

        ~DataExportDAL()
        {
            Dispose(false);
        }
    }
}
