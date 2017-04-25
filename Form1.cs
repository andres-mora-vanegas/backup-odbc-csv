using System;
using System.ComponentModel;
using System.Data;
using System.Text;
using System.Windows.Forms;
using System.Data.Odbc;
using System.IO;
using System.Threading;

namespace cronCsv
{
    public partial class Form1 : Form
    {
        private BackgroundWorker BackgroundWorker1 = new BackgroundWorker();

        public DateTime beginDay =new DateTime();        
        public string odbcData = "Driver={SQL Server}; DSN=ODBCNAME;SERVER=SERVERNAME;UID=USER;PWD=PASSWORD";        

        public Form1()
        {
            InitializeComponent();
            BackgroundWorker1.DoWork += backgroundWorker1_DoWork;
            BackgroundWorker1.ProgressChanged += backgroundWorker1_ProgressChanged;
            BackgroundWorker1.WorkerReportsProgress = true;

            if (BackgroundWorker1.IsBusy != true)
            {
                BackgroundWorker1.RunWorkerAsync();
            }            
            lbStart.Text += DateTime.Today.ToString("dd-MM-yyyy")+" ";
            lbStart.Text += DateTime.Now.ToString("HH:mm:ss");
        }

        public void baseOperation(string firstLine, string[] secondLine, string query, string fileName)
        {
            OdbcConnection DbConnection = new OdbcConnection(odbcData);            
            DbConnection.Open();
            OdbcCommand DbCommand = DbConnection.CreateCommand();
            DbCommand.CommandTimeout = 0;
            DbCommand.CommandType = CommandType.StoredProcedure;
            DbCommand.CommandText = query;
            OdbcDataReader DbReader = DbCommand.ExecuteReader();

            UTF8Encoding utf8 = new UTF8Encoding();

            int fCount = DbReader.FieldCount;
            var newLine = firstLine;
            string filePath = @"./" + fileName + ".csv";
            //before your loop
            var csv = new StringBuilder();
            csv.AppendLine(newLine);

            while (DbReader.Read())
            {
                string secondLineM = "";
                for (int x = 0; x < secondLine.Length; x++)
                {
                    secondLineM += DbReader[secondLine[x]] + ";";
                }
                Byte[] encodedBytes = utf8.GetBytes(secondLineM);
                newLine = utf8.GetString(encodedBytes);

                csv.AppendLine(newLine);
            }
            //after your loop
            File.WriteAllText(filePath, csv.ToString());

            DbReader.Close();
            DbCommand.Dispose();
            DbConnection.Close();
        }

        public void EnrollmentStudent()
        {

            string query = @"SELECT 
                                te.EnrollmentId id,
                                te.UserId,
                                te.EntityId,
                                te.Flags,
                                te.Status,
                                CONVERT(VARCHAR(25),te.StartDate,21) 'StartDate',
                                CONVERT(VARCHAR(25),te.EndDate,21) 'EndDate',
                                CONVERT(VARCHAR(25),te.CreationDate,21) 'CreationDate',
                                te.CreationBy,        
                                tp.Reference,
                                concat(tp.FirstName,' ',tp.LastName) teacher,
                                tp.FirstName name,
                                tp.LastName lastName,
                                ts.Title section,SectionId sectionId,
		                        tc.title courseName,
                                tua.Email email,
                                tua.Username 'user',
		                        CONVERT(VARCHAR(25),ts.StartDate,21) 'startDateSection',
                                CONVERT(VARCHAR(25),ts.endDate,21) 'endDateSection'
                                FROM
                                enrollment te,
                                person tp,
                                section ts,
                                userAccount tua,
		                        course tc
                                WHERE
                                te.UserId=tp.PersonId
                                AND
                                te.EntityId=ts.SectionId                      
                                AND
                                te.Status='1'
                                AND
                                ts.StartDate>'2015-01-01'
		                        AND
		                        ts.EndDate>=getdate ()
                                AND
                                tp.PersonId=tua.PersonId
		                        AND		
		                        (
			                        te.flags='2097153'		
		                        or
			                        te.flags='552161247232'
		                        or
			                        te.flags='553243377664'
		                        )
		                        AND
		                        ts.courseId=tc.courseId";

            string firstLine = "id;UserId;EntityId;Flags;Status;StartDate;EndDate;CreationDate;CreationBy;Reference;teacher;name;lastName;section;sectionId;courseName;email;user;startDateSection;endDateSection";
            string[] secondLine = new string[20] { "id", "UserId", "EntityId", "Flags", "Status", "StartDate", "EndDate", "CreationDate", "CreationBy", "Reference", "teacher", "name", "lastName", "section", "sectionId", "courseName", "email", "user", "startDateSection", "endDateSection" };
            string fileName = "studentEnrollmentPersonSectionUserAccountCourse";
            baseOperation(firstLine, secondLine, query, fileName);
        }

        public void EnrollmentTutor()
        {

            string query = @"SELECT 
                                te.EnrollmentId id,
                                te.UserId,
                                te.EntityId,
                                te.Flags,
                                te.Status,
                                CONVERT(VARCHAR(25),te.StartDate,21) 'StartDate',
                                CONVERT(VARCHAR(25),te.EndDate,21) 'EndDate',
                                CONVERT(VARCHAR(25),te.CreationDate,21) 'CreationDate',
                                te.CreationBy,
                                tp.Reference,
                                concat(tp.FirstName,' ',tp.LastName) teacher,
                                tp.FirstName name,
                                tp.LastName lastName,
                                ts.Title section,SectionId sectionId,
		                        tc.title courseName,
                                tua.Email email,
                                tua.Username 'user',
                                CONVERT(VARCHAR(25),ts.StartDate,21) 'startDateSection',
                                CONVERT(VARCHAR(25),ts.endDate,21) 'endDateSection',
                                CONVERT(VARCHAR(25),tua.lastLoginDate,21) lastLogin
                                FROM
                                enrollment te,
                                person tp,
                                section ts,
                                userAccount tua,
		                        course tc		
                                WHERE
                                te.UserId=tp.PersonId
                                AND
                                te.EntityId=ts.SectionId        		         
                                AND
                                te.Status='1'
                                AND
                                ts.StartDate>'2015-01-01'
                                AND
                                tp.PersonId=tua.PersonId
		                        AND
		                        (
		                        te.flags='552161247232'
		                        or
		                        te.flags='553243377664'
		                        )
		                        AND
		                        ts.courseId=tc.courseId";

            string firstLine = "id;UserId;EntityId;Flags;Status;StartDate;EndDate;CreationDate;CreationBy;Reference;teacher;name;lastName;section;sectionId;courseName;email;user;startDateSection;endDateSection;lastLogin";
            string[] secondLine = new string[21] { "id", "UserId", "EntityId", "Flags", "Status", "StartDate", "EndDate", "CreationDate", "CreationBy", "Reference", "teacher", "name", "lastName", "section", "sectionId", "courseName", "email", "user", "startDateSection", "endDateSection", "lastLogin" };
            string fileName = "enrollmentPersonSectionUserAccountCourse";
            baseOperation(firstLine, secondLine, query, fileName);
        }

        public void UserAccount()
        {
            string query = @"select
	                            tp.personId,
	                            tp.firstName,
	                            tp.lastName,
	                            tp.reference identification,
	                            tua.userName,
	                            tua.email	
	                            from 
	                            person tp,userAccount tua
	                            where
	                            tua.personId=tp.personId";

            string firstLine = "personId;firstName;lastName;identification;userName;email";
            string[] secondLine = new string[6] { "personId", "firstName", "lastName", "identification", "userName", "email" };
            string fileName = "userAccountPerson";
            baseOperation(firstLine, secondLine, query, fileName);
        }

        public void finalice() {

            DateTime finalDay=new DateTime();

            string filePath = @"./log";            
            var csv = new StringBuilder();            
            string minutes = (this.beginDay - finalDay).Minutes.ToString();
            string m = DateTime.Today.ToString("dd-MM-yyyy") + " ";
            m += DateTime.Now.ToString("HH:mm:ss");
            string firstLine = "Inicio: "+ lbStart.Text + "\nFin: "+m+" \nTotal minutos "+minutes;
            csv.AppendLine(firstLine);
            File.WriteAllText(filePath, csv.ToString());

        }
        // This event handler is where the time-consuming work is done.
        private void backgroundWorker1_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker worker = sender as BackgroundWorker;
            UserAccount();
            worker.ReportProgress(33);
            EnrollmentStudent();
            worker.ReportProgress(66);
            EnrollmentTutor();
            worker.ReportProgress(100);
            finalice();
            System.Threading.Thread.Sleep(2000);
            Application.Exit();
        }

        // This event handler updates the progress.
        private void backgroundWorker1_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            tx1.Text = "Procesado al " + (e.ProgressPercentage.ToString() + "%");
        }

        // This event handler deals with the results of the background operation.
        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            if (e.Cancelled == true)
            {
                tx1.Text = "Canceled!";
            }
            else if (e.Error != null)
            {
                tx1.Text = "Error: " + e.Error.Message;
            }
            else
            {
                tx1.Text = "Done!";
            }
        }

    }
}
