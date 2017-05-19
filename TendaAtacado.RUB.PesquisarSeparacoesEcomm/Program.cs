using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Npgsql;
using System.Diagnostics;
using System.Configuration;

namespace TendaAtacado.RUB.PesquisarSeparacoesEcomm
{

    public class Separacao
    {

        public string CodigoSeparacao{ get; set; }
        public int CodigoLoja{ get; set;}

    }


    class Program
    {

        static System.Data.SqlClient.SqlConnection c = new SqlConnection(ConfigurationManager.ConnectionStrings["SQLSERVER_CENTRAL"].ConnectionString);

        
        static void Main(string[] args)
        {

            GravarLog("Iniciando varredura.", EventLogEntryType.Information);

            List<Separacao> ss = new List<Separacao>();

            //int teste;

            //teste = 10000083;

            ss = ConsultarSeparacoes();

            foreach (var item in ss)
            {
                Verificar(item.CodigoLoja, item.CodigoSeparacao);
            }

            GravarLog("Finalizando o processo.", EventLogEntryType.Information);
            
        }




        static List<Separacao> ConsultarSeparacoes()
        {

            List<Separacao> s = new List<Separacao>();

            try
            {
                int tempo = Convert.ToInt32(ConfigurationManager.AppSettings["TempoParaVerificacao"]);

                System.Data.SqlClient.SqlCommand comando = new SqlCommand();
                comando.CommandTimeout = 0;
                comando.CommandType = System.Data.CommandType.Text;
                comando.CommandText = "select R.id_loja, R.codigo " +
                                      "from " +
                                      "db_rub_central.dbo.rub_separacao R " +
                                      "left join db_intranet2.dbo.TBLVTEX_SEPARACAO_CAPTADA_PELO_RUB C on (R.codigo collate SQL_Latin1_General_CP1_CI_AS = C.orderid) " +
                                      "WHERE R.LIDO = 1 and C.orderid is null " +
                                      "and datediff(minute, R.data_hora, getdate()) > " + tempo.ToString();
                comando.Connection = c;
                c.Open();

                System.Data.SqlClient.SqlDataReader r = comando.ExecuteReader();
                while (r.Read())
                {
                    Separacao s1 = new Separacao();
                    s1.CodigoLoja = (int)r[0];
                    s1.CodigoSeparacao = (string)r[1];

                    s.Add(s1);

                }

                r.Close();
            }
            catch (System.Data.SqlClient.SqlException e)
            {
                GravarLog(e.Message, EventLogEntryType.Error);
            }

            if (c.State == System.Data.ConnectionState.Open)
            {
                c.Close();
            }

            return s;

        }


        static string IP_LOJA(int loja)
        {
            string resultado = "";

            try
            {

                c.Open();

                System.Data.SqlClient.SqlCommand co = new SqlCommand();
                co.CommandText = "select path_comum from zeusretail.dbo.tab_loja where cod_loja = " + loja.ToString();
                co.Connection = c;

                string ip = (string)co.ExecuteScalar();

                ip = ip.Replace("path_comum" + loja.ToString(), String.Empty);
                ip = ip.Replace("\\", String.Empty);
                ip = ip.Replace("\\\\", String.Empty);

                string[] ip_loja = ip.Split('.');

                resultado = ip_loja[0] + "." + ip_loja[1] + "." + ip_loja[2] + ".14";

                            
            }
            catch(System.Data.SqlClient.SqlException e)
            {
                GravarLog(e.Message, EventLogEntryType.Error);
            }

            if (c.State == System.Data.ConnectionState.Open)
            {
                c.Close();
            }

            return resultado;

        }


        static void AtualizarControle( string codigo)
        {
            try
            {
                c.Open();

                System.Data.SqlClient.SqlCommand co = new SqlCommand("insert into db_intranet2.dbo.TBLVTEX_SEPARACAO_CAPTADA_PELO_RUB(ORDERID) values (" + "'" + codigo + "'" + ")", c);
                co.ExecuteNonQuery();

            }
            catch( SqlException e)
            {
                GravarLog(e.Message, EventLogEntryType.Error);
            }

            if (c.State == System.Data.ConnectionState.Open)
            {
                c.Close();
            }


        }

        static void ReenviarControle(string codigo)
        {
            try
            {
                c.Open();
                string comando = "update db_rub_central.dbo.rub_separacao set lido = 0 where codigo = '" + codigo + "'";
                System.Data.SqlClient.SqlCommand co = new SqlCommand(comando, c);
                co.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                GravarLog(e.Message, EventLogEntryType.Error);
            }

            if (c.State == System.Data.ConnectionState.Open)
            {
                c.Close();
            }
        }


        static void Verificar(int loja, string codigo)
        {
            string ip = IP_LOJA(loja);

            GravarLog("Iniciando verificação do pedido " + codigo, EventLogEntryType.Information);

            string s = "Server=" + ip.ToString() + ";Port=5432;Database=rub;User Id=gic;Password=gicbrasil";

            Npgsql.NpgsqlConnection postgres = new NpgsqlConnection(s);

            try
            {

                NpgsqlCommand co = new NpgsqlCommand("select count(*) as qtd from separacao where codigo = '" + codigo + "'");
                co.Connection = postgres;
                co.CommandType = System.Data.CommandType.Text;
                postgres.Open();



                if (co.ExecuteScalar().ToString() == "1")
                {
                    AtualizarControle(codigo);
                }
                else
                {
                    ReenviarControle(codigo);
                    GravarLog("Pedido: " + codigo + " - reenviado para separação na loja " + loja.ToString() , EventLogEntryType.Warning);
                }
            }
            catch (Npgsql.NpgsqlException e)
            {
                GravarLog(e.Message, EventLogEntryType.Error);
            }

            if (postgres.State == System.Data.ConnectionState.Open)
            {
                postgres.Close();
            }

        }


        static void GravarLog(string evento, EventLogEntryType tipo)
        {
            string sSource;
            string sLog;

            sSource = "TendaAtacado.RUB.PesquisarSeparacoesEcomm";
            sLog = "Application";

            if (!EventLog.SourceExists(sSource))
                EventLog.CreateEventSource(sSource, sLog);

            EventLog.WriteEntry(sSource, evento, tipo, 234);
        }






    }




}
