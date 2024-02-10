
// WordPress DB Connectionstring
string connectionString = @"server=localhost;userid=user;password=password;database=db"; 

ExportComment.Export(connectionString).GetAwaiter().GetResult();