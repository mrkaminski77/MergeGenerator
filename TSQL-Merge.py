# MergeGenerator.py
# by David Leyden
# 
import sys
import clr
sys.path.append(r"C:\Program Files (x86)\Microsoft SQL Server\100\SDK\Assemblies")
clr.AddReferenceToFile('Microsoft.SqlServer.Smo.dll')
clr.AddReferenceToFile('Microsoft.SqlServer.SqlEnum.dll')
clr.AddReferenceToFile('Microsoft.SqlServer.ConnectionInfo.dll')
import Microsoft.SqlServer.Management.Smo as SMO
import Microsoft.SqlServer.Management.Common as Common
import System.Collections

targetSchema = sys.argv[1]
targetTable = sys.argv[2]
db_server = SMO.Server("SCSBENSQLDEV01")
db = db_server.Databases["ATOReporting_DEV"]
for t in db.Tables:
	if t.Schema == targetSchema:
		if t.Name == targetTable:
			targ = t
columns = targ.Columns

print ("CREATE PROCEDURE [" + targetSchema + "].[Merge_" + targetTable + "] @startDate DATE, @endDate DATE")
print ("WITH EXECUTE AS OWNER")
print ("AS")
print ("INSERT INTO [aux].[sp_execution]")
print ("VALUES (")
print ("\t@@PROCID,")
print ("\tGETDATE(),")
print ("\tUSER_ID()")
print ("\t);\n");

print ("DECLARE @ReportRange NVARCHAR(MAX);")
print ("DECLARE @errorReport NVARCHAR(MAX);")
print ("DECLARE @NewLine AS CHAR(2) = CHAR(13) + CHAR(10);");
print ("DECLARE @emailRecipients NVARCHAR(MAX);\n")

print ("SELECT @emailRecipients = [value]")
print ("FROM [aux].[config]")
print ("WHERE [name] = 'error_notifications_distribution_list';\n")

print ("DECLARE @t TABLE (")
counter = 1
for f in targ.Columns:
	if counter == 1:
		print ("\t [" + f.Name + "] " + f.DataType.Name + "(" + str(f.DataType.MaximumLength)) + ")"		
		counter += 1
	else:
		print ("\t ,[" + f.Name + "] " + f.DataType.Name + "(" + str(f.DataType.MaximumLength)) + ")"		
print (");\n")


print ("BEGIN TRY")
print ("\tBEGIN TRANSACTION;")

print("\t\tINSERT INTO @t")
print ("\t\tSELECT")
counter = 1
for f in targ.Columns:
	if counter == 1:
		print ("\t\t\t [" + f.Name + "]")			
		counter += 1
	else:
		print ("\t\t\t ,[" + f.Name + "]")
print ("\t\tFROM <source,,>;")


print ("\t\tWITH cte AS (")
print ("\t\t\t SELECT")
counter = 1
for f in targ.Columns:
	if counter != targ.Columns.Count:
		print ("\t\t\t\t[" + f.Name + "] = [" + f.Name +"],")
		counter += 1
	else:
		print ("\t\t\t\t[" + f.Name + "] = [" + f.Name +"]")
		print ("\t\t\tFROM @t")
print ("\t\t)")
print ("\t\tMERGE [" + targ.Schema + "].[" + targ.Name + "] AS targ")
print ("\t\tUSING cte as src")
print ("\t\tON ")
counter = 1
for f in targ.Columns:
	if f.InPrimaryKey :
		if counter == 1:
			print ("\t\t\t\t src.[" + f.Name + "] = targ.[" + f.Name + "]")			
			counter += 1
		else:
			print ("\t\t\t\t AND src.[" + f.Name + "] = targ.[" + f.Name + "]")
print ("\t\tWHEN MATCHED AND EXISTS (")
print ("\t\t\tSELECT")
counter = 1
for f in targ.Columns:
	if not f.InPrimaryKey :
		if counter == 1:
			print ("\t\t\t\t src.[" + f.Name + "]")			
			counter += 1
		else:
			print ("\t\t\t\t ,src.[" + f.Name + "]")			
print ("\t\t\tEXCEPT")
print ("\t\t\tSELECT")
counter = 1
for f in targ.Columns:
	if not f.InPrimaryKey :
		if counter == 1:
			print ("\t\t\t\t targ.[" + f.Name + "]")			
			counter += 1
		else:
			print ("\t\t\t\t ,targ.[" + f.Name + "]")			
print ("\t\t)")
print ("\t\tTHEN UPDATE SET")
counter = 1
for f in targ.Columns:
	if not f.InPrimaryKey :
		if counter == 1:
			print ("\t\t\t\t [" + f.Name + "] = src.[" + f.Name + "]")			
			counter += 1
		else:
			print ("\t\t\t\t ,[" + f.Name + "] = src.[" + f.Name + "]")					

print ("\t\tWHEN NOT MATCHED BY TARGET THEN INSERT (")
counter = 1
for f in targ.Columns:
	if counter == 1:
		print ("\t\t\t\t [" + f.Name + "]")			
		counter += 1
	else:
		print ("\t\t\t\t ,[" + f.Name + "]")		
print ("\t\t)")
print "\t\tVALUES ("
counter = 1
for f in targ.Columns:
	if counter == 1:
		print ("\t\t\t\t src.[" + f.Name + "]")			
		counter += 1
	else:
		print ("\t\t\t\t ,src.[" + f.Name + "]")
print ("\t\t)")
print ("\t\tWHEN NOT MATCHED BY SOURCE \n\t\t\t THEN DELETE;")
print ("\tCOMMIT;")
print ("END TRY")
print ("BEGIN CATCH")
print ("\tROLLBACK;")
print ("\tEXECUTE AS CALLER;")
print ("\tSET @errorReport = N'" + targ.Name + " merge failed.'")
print ("\tEXEC [aux].[email_sp_error] @errorReport, @emailRecipients;")
print ("\tREVERT;")
print ("END CATCH;")

