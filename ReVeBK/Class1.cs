using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;

namespace ReVeBK
{
    public class DBBK
    {
        OleDbConnection dbcon = new OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source=RVDB.accdb");
        OleDbConnection dbconverw = new OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source=RVDB.accdb");
        OleDbCommand dbcmd = null;
        OleDbDataReader dataReader = null;
        OleDbDataAdapter da = null;
        DataSet ds = new DataSet();

        public DataSet LeseArtikel(int artIndex)
        {
            dbcon.Open();
            ds.Clear();
            if (artIndex == 0)
            {
                da = new OleDbDataAdapter("SELECT * from Artikel1 ", dbcon);
            }
            else
            {
                da = new OleDbDataAdapter("SELECT * from Artikel1 WHERE Art1Nr = "+artIndex+"", dbcon);
            }

            da.Fill(ds, "sämtlicheartikel");
            dbcon.Close();
            return ds;
        }

        public void EinfuegenArtikel(DateTime artDat, string artBez, int artBestand, double artStkp)
        {
            try
            {
                dbcon.Open();
                dbcmd = new OleDbCommand("SELECT MAX(Art1Nr) FROM Artikel1", dbcon);
                dataReader = dbcmd.ExecuteReader();
                dbcmd = null;
                dataReader.Read();
                int artIndex = dataReader.GetInt32(0);

                //Einfügen des Datensatzes
                dbcmd = new OleDbCommand("INSERT INTO Artikel1 (Art1Nr, Art1StartDat, Art1Bez, Art1Bestand, Art1Stückpreis) values (" + (artIndex + 1) + ",'" + artDat.ToString("dd.MM.yyyy") + "','" + artBez + "'," + artBestand + ",'" + artStkp + "')", dbcon);
                dbcmd.ExecuteNonQuery();
                dbcon.Close();
                return;
            }
            catch(Exception a)
            {
                throw a;
            }
            
        }

        public string AendernArtikelPreis(int gridIndex, double neuPreis)
        {
            try
            {
                dbcmd = new OleDbCommand("SELECT * FROM Artikel1 WHERE Art1Nr=" + gridIndex +";", dbcon);
                dbcon.Open();
                dataReader = null;
                dataReader = dbcmd.ExecuteReader();
                dataReader.Read();
            }
            catch (Exception a)
            {
                throw a;
            }
            try
            {
                dbcmd = new OleDbCommand("INSERT INTO Artikel1 (Art1Nr, Art1StartDat, Art1Bez, Art1Bestand, Art1Stückpreis) values (" + dataReader.GetInt32(0) + ",'" + DateTime.Today.Date.ToString("dd.MM.yyyy") + "','" + dataReader.GetString(2) + "'," + dataReader.GetInt32(3) + "," + neuPreis + ")", dbcon);
                dbcmd.ExecuteNonQuery();
            }
            catch(Exception a)
            {
                throw a;
            }
            finally
            {
                dbcon.Close();
            }
            return dbcmd.ToString(); ;
        }

        public OleDbDataReader LeseSpezArtikel(int artNr)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * from Artikel1 WHERE Art1Nr = "+artNr+" ORDER BY Art1startdat DESC",dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public OleDbDataReader LeseSpezArtikel(int artNr, DateTime artDat)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * from Artikel1 WHERE Art1Nr = " + artNr + " AND Art1StartDat LIKE '"+ artDat.ToString("dd.MM.yyyy") +"'", dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public void AktualisiereBestand(int index, int neuBestand)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("UPDATE Artikel1 SET Art1Bestand = " + neuBestand + " WHERE Art1Nr = "+ index, dbcon);
            dbcmd.ExecuteNonQuery();
            return;
        }

        public DataSet LeseKunden(string suchWert, string suchAttr)
        {
            ds.Clear();
            dbcon.Open();
            da = new OleDbDataAdapter("SELECT * from Kunden WHERE "+suchAttr+" LIKE '%" + suchWert + "%'", dbcon);
            da.Fill(ds, "kunden");
            dbcon.Close();
            return ds;
        }

        public DataSet LeseKunden(int suchWert,string suchAttr)
        {
            ds.Clear();
            da = new OleDbDataAdapter("SELECT * from Kunden WHERE " + suchAttr + " = " + suchWert + "", dbcon);
            da.Fill(ds, "kunden");
            dbcon.Close();
            return ds;
        }

        public DataSet LeseKunden()
        {
            dbcon.Open();
            ds.Clear();
            da = new OleDbDataAdapter("SELECT * from Kunden", dbcon);
            da.Fill(ds, "kunden");
            dbcon.Close();
            return ds;
        }

        //frage einzelnen Kunden anhand der Kundennr ab (davon den aktuellsten Datensatz)
        public OleDbDataReader LeseSpezKunden(int kdIndex)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * FROM Kunden WHERE KundenNr = "+kdIndex+" ORDER BY KundenDatAktual DESC", dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public OleDbDataReader LeseSpezKunden(int kdIndex, DateTime kdDat)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * FROM Kunden WHERE KundenNr = " + kdIndex + "AND KundenDatAktual LIKE '"+kdDat.ToString("dd.MM.yyyy")+"'", dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        //Lege neuen Datensatz mit aktualisierten Kundendaten an
        public void AendereKunden(int kdnr,string firma,string name,string tel, string adr)
        {
            dbcon.Open();
            try
            {
                dbcmd = new OleDbCommand("INSERT INTO Kunden(KundenNr,KundenDatAktual, KundenFirmenName, KundenAnspName, KundenTel, KundenAdresse) Values (" + kdnr + ",'" + DateTime.Today.Date.ToString("dd.MM.yyyy") + "','" + firma + "','" + name + "','" + tel + "','" + adr + "')", dbcon);
                dbcmd.ExecuteNonQuery();
            }
            catch
            {
                throw new ArgumentException("Kundendaten dürfen nur ein mal am Tag geändert werden");
            }
            dbcon.Close();
        }

        
        public void EinfuegenKunde(string kundenFirmaName, string kundenAnspName,string kundenTel,string kundenAdresse)
        {
            try
            {
                dbcon.Open();
                dbcmd = new OleDbCommand("SELECT MAX(KundenNr)  FROM Kunden", dbcon);
                dataReader = dbcmd.ExecuteReader();
                dataReader.Read();
                dbcmd = new OleDbCommand("INSERT INTO Kunden (KundenNr, KundenDatAktual, KundenFirmenName, KundenAnspName, KundenTel, KundenAdresse) values (" + (dataReader.GetInt32(0) + 1) + ",'" + DateTime.Today.Date.ToString("dd.MM.yyyy") + "','" + kundenFirmaName + "','" + kundenAnspName + "','" + kundenTel + "','" + kundenAdresse + "')", dbcon);
                dbcmd.ExecuteNonQuery();
                dbcon.Close();
            }
            catch(Exception a)
            {
                throw a;
            }
        }

        //Lese aktuellste Verwaltungsdaten
        public OleDbDataReader LeseVerwaltungsDaten()
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT MAX(VerwaltungsNr) FROM Verwaltungsbasis", dbcon);
            dataReader = dbcmd.ExecuteReader();
            dbcmd = null;
            dataReader.Read();
            dbcmd = new OleDbCommand("SELECT * FROM Verwaltungsbasis WHERE VerwaltungsNr = " + (dataReader.GetInt32(0)), dbcon);
            dataReader = dbcmd.ExecuteReader();
            
            return dataReader;
        }

        public OleDbDataReader LeseSpezVerwaltungsDaten(int vwnr)
        {
            //Command, DataReader und Connection müssen andere sein um parallel zu den Globalen verfügbar zu sein
            dbconverw.Open();
            OleDbCommand dbcmdverw = new OleDbCommand("SELECT * FROM Verwaltungsbasis WHERE VerwaltungsNr = " + vwnr, dbcon);
            OleDbDataReader dataReaderverw = dbcmdverw.ExecuteReader();
            return dataReaderverw;
        }

        public void EinfuegenVerwaltungsDaten(int GsUmsSchw, int ReSchw, int mahnfrist)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT MAX(VerwaltungsNr) FROM Verwaltungsbasis",dbcon);
            dataReader = dbcmd.ExecuteReader();
            dbcmd = null;
            dataReader.Read();
            dbcmd = new OleDbCommand("INSERT INTO Verwaltungsbasis(VerwaltungsNr,Gesamtumsatzschwelle,Rechnungsschwelle,Mahnfrist) Values("+(dataReader.GetInt32(0)+1)+","+GsUmsSchw+","+ReSchw+","+mahnfrist+")",dbcon);
            dbcmd.ExecuteNonQuery();
            dbcon.Close();
        }

        public OleDbDataReader LeseMwst()
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT MAX(MwstNr) FROM Mwst", dbcon);
            dataReader = dbcmd.ExecuteReader();
            dbcmd = null;
            dataReader.Read();
            dbcmd = new OleDbCommand("SELECT * FROM Mwst WHERE MwstNr = "+ (dataReader.GetInt32(0)),dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public OleDbDataReader LeseSpezMwst(int mwstnr)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * FROM Mwst WHERE MwstNr = "+ mwstnr +"", dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public void EinfuegenMwst(int mwstSatz)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT MAX(MwstNr) FROM Mwst",dbcon);
            dataReader = dbcmd.ExecuteReader();
            dataReader.Read();
            dbcmd = null;
            dbcmd = new OleDbCommand("INSERT INTO Mwst(MwstNr,MwstSatz) VALUES("+(dataReader.GetInt32(0)+1)+","+mwstSatz+")", dbcon);
            dbcmd.ExecuteNonQuery();
            dbcon.Close();
        }

        public OleDbDataReader LeseMaxRechNr()
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT MAX(RNr) FROM Rechnung",dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public void ErstellenRechnung(int rnr,bool bezahlt,int kdnr,DateTime kddat,int mwstnr, int vwnr, double gesamtBetr)
        {
            try
            {
                dataReader = LeseVerwaltungsDaten();
                dataReader.Read();
                int mahnfrist = dataReader.GetInt32(3);
                Schliessen();
                DateTime faelligDatum = DateTime.Today.Date.AddDays(mahnfrist);
                dbcon.Open();
                dbcmd = new OleDbCommand("INSERT INTO Rechnung(RNr,RDatum,RFälligDatum,RBezahlt,RMahnstufe,RKundenNr,RKundenDatAktual,RMwstNr,RVerwaltungsNr,RGesamtBetr) VALUES (" + rnr + ",'" + DateTime.Today.Date.ToString("dd.MM.yyyy") + "','" + faelligDatum.Date.ToString("dd.MM.yyyy") + "'," + bezahlt + "," + 0 + "," + kdnr + ",'" + kddat.Date.ToString("dd.MM.yyyy") + "'," + mwstnr + "," + vwnr + ",'" + gesamtBetr + "')", dbcon);
                dbcmd.ExecuteReader();
                dbcon.Close();
            }
            catch(Exception a)
            {
                throw a;
            }
            
        }

        public void ErstellenRechnungsPosition(int art1Nr,int rnr,int menge)
        {
            dataReader = LeseSpezArtikel(art1Nr);
            dataReader.Read();
            dbcmd = new OleDbCommand("INSERT INTO Artikel2 (Art1Nr, RNr, Art1StartDat, Menge) VALUES (" + art1Nr + "," + rnr + ",'" + dataReader.GetDateTime(1).Date.ToString("dd.MM.yyyy") + "'," + menge + ")", dbcon);
            Schliessen();
            dbcon.Open();
            dbcmd.ExecuteNonQuery();
            dbcon.Close();
        }

        public void AkualisiereBestand(int artNr, int menge)
        {
            
            int alterBestand;
            dataReader = LeseSpezArtikel(artNr);
            dataReader.Read();
            alterBestand = dataReader.GetInt32(3);
            dbcmd = new OleDbCommand("UPDATE Artikel1 SET Art1Bestand =" + (alterBestand-menge) + " WHERE art1Nr = " + artNr + "",dbcon);
            dbcmd.ExecuteNonQuery();
            dbcon.Close();
        }

        public DataSet LesenRechnungsPosition(int rnr)
        {
            ds.Clear();
            dbcon.Open();
            da = new OleDbDataAdapter("SELECT * from Artikel2 WHERE RNr =" + rnr +"",dbcon);
            da.Fill(ds, "SpezArtikel");
            dbcon.Close();
            return ds;
        }

        public DataSet LesenRechnung(int kdnr)
        {
            ds.Clear();
            dbcon.Open();
            da = new OleDbDataAdapter("SELECT * from Rechnung WHERE RKundenNr = " + kdnr + "", dbcon);
            da.Fill(ds, "rechnung");
            dbcon.Close();
            return ds;
        }

        public OleDbDataReader LesenSpezRechnung(int rnr)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * from Rechnung WHERE rnr = "+rnr+"", dbcon);
            dataReader = dbcmd.ExecuteReader();
            return dataReader;
        }

        public void AktualisierenBezahlung(int rnr ,bool bezahlt)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("UPDATE Rechnung SET RBezahlt = " + bezahlt + " WHERE RNr = " + rnr+"", dbcon);
            dbcmd.ExecuteNonQuery();
            dbcon.Close();
        }

        public bool CheckVipStatus(int kdnr)
        {
            bool istBezahlt = false;
            double kundenSumme = 0;
            ds = LesenRechnung(kdnr);
            for (int c = 0; c < ds.Tables[0].Rows.Count; c++)
            {
                istBezahlt = Convert.ToBoolean(ds.Tables[0].Rows[c][3]);
                if (istBezahlt)
                {
                    kundenSumme += Convert.ToDouble(ds.Tables[0].Rows[c][9]);
                }
            }
            dataReader = LeseVerwaltungsDaten();
            dataReader.Read();
            int vipSchwelle = dataReader.GetInt32(1);
            Schliessen();
            if (kundenSumme >= vipSchwelle)
            {
                return true;
            }
            else
            {
                return false;
            }
 
        }

        public void MahnPrüfung()
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * from Rechnung ORDER BY Rnr ASC", dbcon);
            dataReader = dbcmd.ExecuteReader();
      
            int c = 0;
            while(dataReader.Read())
            {
                int mahnstufe = 0;
                OleDbDataReader dataReaderverw = LeseSpezVerwaltungsDaten(dataReader.GetInt32(8));
                dataReaderverw.Read();
                int mahnfrist = dataReaderverw.GetInt32(3);
                dbconverw.Close();
                TimeSpan days = DateTime.Today - dataReader.GetDateTime(1);
                if (days.TotalDays > 1 * mahnfrist && days.TotalDays < 2 * mahnfrist)
                {
                   mahnstufe = 1;
                }
                else if (days.TotalDays > 2 * mahnfrist && days.TotalDays < 3 * mahnfrist)
                {
                    mahnstufe = 2;
                }
                else if (days.TotalDays > 3 * mahnfrist)
                {
                    mahnstufe = 3;
                }
                dbcmd = new OleDbCommand("Update Rechnung SET RMahnstufe = " + mahnstufe + " WHERE RNr = " + c+1, dbcon);
                dbcmd.ExecuteNonQuery();
                c++;
                
            }
            dbcon.Close();
            return;

        }

        public bool CheckOffeneRechnungen(int kdnr)
        {
            dbcon.Open();
            dbcmd = new OleDbCommand("SELECT * FROM Rechnung WHERE RKundenNr = "+kdnr,dbcon);
            dataReader = dbcmd.ExecuteReader();
            while (dataReader.Read())
            {
                if (!dataReader.GetBoolean(3))
                {
                    dbcon.Close();
                    return true;
                }
            }
            dbcon.Close();
            return false;
        }


        public void Schliessen()
        {
            dbcon.Close();
        }

    }

}

