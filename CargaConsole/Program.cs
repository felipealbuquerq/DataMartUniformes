using System;
using System.Data.SqlClient;
using System.Text;

namespace AtendimentosDiariosPgmFullS
{
    class Program
    {
        //Declarar o Vetor
        //Quantidade de Atendimentos e fornecimentos
        //com os seguintes Indices
        //(Mes, Dia, Codigo Uniforme, Numero Uniforme)
        #region Variaveis 
        static int tamanhoMes = 13;
        static int tamanhoDia = 32;
        static int tamanhoCodigo = 32;
        static int tamanhoNro = 506;

        static int empresa = 0;
        static int filial = 0;
        static int ano = 0;
        static int mes = 0;
        static int dia = 0;
        static int codigo = 0;
        static int nro = 0;

        static int[,,,] atendimentos =
        new int[tamanhoMes, tamanhoDia, tamanhoCodigo, tamanhoNro];
        static int[,,,] fornecimentos =
        new int[tamanhoMes, tamanhoDia, tamanhoCodigo, tamanhoNro];
        static String[] descricaoCodigo = new String[40];

        //Conexão do laboratório
        //static String stringDeConexao = "server=(local)\\sqlexpress;database=Uniformes;user=sa;pwd=uniesp";
        //Conexão com autenticação do windows
        static String stringDeConexao = "Server = (local)\\sql; Database = Uniformes; Trusted_Connection = True";
        #endregion

        static void InicializarVetores()
        {
            try
            {
                for (mes = 0; mes < tamanhoMes; mes++)
                {
                    for (dia = 0; dia < tamanhoDia; dia++)
                    {
                        for (codigo = 0; codigo < tamanhoCodigo; codigo++)
                        {
                            for (nro = 0; nro < tamanhoNro; nro++)
                            {
                                atendimentos[mes, dia, codigo, nro] = 0;
                                fornecimentos[mes, dia, codigo, nro] = 0;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao iniciar vetores. {0}", ex.Message));
            }
        }

        static void ListarOsVetores()
        {
            try
            {
                for (mes = 0; mes < tamanhoMes; mes++)
                {
                    for (dia = 0; dia < tamanhoDia; dia++)
                    {
                        for (codigo = 0; codigo < tamanhoCodigo; codigo++)
                        {
                            for (nro = 0; nro < tamanhoNro; nro++)
                            {
                                if (fornecimentos[mes, dia, codigo, nro] != 0)
                                {
                                    Console.WriteLine("atendimentos -"
                                    + mes + " "

                                    + dia + " "
                                    + codigo + " "
                                    + nro + " => "
                                    + atendimentos[mes, dia, codigo, nro] + " ");
                                    Console.WriteLine("fornecimentos -"
                                    + mes + " "
                                    + dia + " "
                                    + codigo + " "
                                    + nro + " => "
                                    + fornecimentos[mes, dia, codigo, nro] + " ");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao listar os vetores. {0}", ex.Message));
            }
        }

        static void CarregarOsVetores()
        {
            SqlConnection conexao3 = new SqlConnection(stringDeConexao);

            try
            {
                conexao3.Open();

                //O objeto StringBuilder otimiza a utilização da memória em comparação a concatenações de string.
                StringBuilder sqlMovimentacao = new StringBuilder();

                int movQtde = 0;

                #region Comando SQL
                sqlMovimentacao.Append("SELECT ");
                sqlMovimentacao.Append("    EmpresaCodigo, ");
                sqlMovimentacao.Append("    FilialCodigo, ");
                sqlMovimentacao.Append("    YEAR(MovimentacaoData) AnoMov, ");
                sqlMovimentacao.Append("    MONTH(MovimentacaoData) MesMov, ");
                sqlMovimentacao.Append("    DAY(MovimentacaoData) DiaMov, ");
                sqlMovimentacao.Append("    UniformeCodigo, ");
                sqlMovimentacao.Append("    UniformeNumero, ");
                sqlMovimentacao.Append("    MovimentacaoQtde ");
                sqlMovimentacao.Append("FROM Movimentacao ");
                sqlMovimentacao.Append(string.Format("WHERE EmpresaCodigo = {0}", empresa));
                sqlMovimentacao.Append(string.Format(" AND FilialCodigo = {0}", filial));
                sqlMovimentacao.Append(string.Format(" AND YEAR(MovimentacaoData) = {0}", ano));
                #endregion

                SqlCommand cmdSQLMov = new SqlCommand(sqlMovimentacao.ToString(), conexao3);

                SqlDataReader rdrMovimentacao = cmdSQLMov.ExecuteReader();

                #region Carregando Vetores
                while (rdrMovimentacao.Read())
                {
                    mes = Convert.ToInt32(rdrMovimentacao["MesMov"].ToString());
                    dia = Convert.ToInt32(rdrMovimentacao["DiaMov"].ToString());
                    codigo = Convert.ToInt32(rdrMovimentacao["UniformeCodigo"].ToString());
                    nro = Convert.ToInt32(rdrMovimentacao["UniformeNumero"].ToString());
                    movQtde = Convert.ToInt32(rdrMovimentacao["MovimentacaoQtde"].ToString());

                    atendimentos[mes, dia, codigo, nro]
                    = atendimentos[mes, dia, codigo, nro] + 1;
                    fornecimentos[mes, dia, codigo, nro]
                    = fornecimentos[mes, dia, codigo, nro] + movQtde;
                }
                #endregion
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao carregar os vetores. {0}", ex.Message));
            }
            finally
            {
                conexao3.Close();
                conexao3.Dispose();
            }
        }

        static void GravarDWDMAtendimentosDiarios()
        {
            String sqlDWDM = "";
            // Conectar o Banco de Dados

            SqlConnection conexao4 = new SqlConnection(stringDeConexao);

            try
            {
                conexao4.Open();
                //---------------------------------------------
                // Gravar o DataWareHouse - Data Mart Uniformes
                //---------------------------------------------

                for (mes = 0; mes < tamanhoMes; mes++)
                {
                    for (dia = 0; dia < tamanhoDia; dia++)
                    {
                        for (codigo = 0; codigo < tamanhoCodigo; codigo++)
                        {
                            for (nro = 0; nro < tamanhoNro; nro++)
                            {
                                if (fornecimentos[mes, dia, codigo, nro] != 0)
                                {
                                    sqlDWDM = "";
                                    sqlDWDM = "INSERT INTO DM_UniformesAtendimentoDiario "
                                    + " (Empresa, Filial, Ano, Mes, Dia, "
                                    + " UniformeDescricaoRdz, UniformeNumero, "
                                    + " QtdeAtendimentos, QtdeFornecida)"
                                    + " VALUES("
                                    + Convert.ToString(empresa) + ", "
                                    + Convert.ToString(filial) + ", "
                                    + Convert.ToString(ano) + ", "
                                    + Convert.ToString(mes) + ", "
                                    + Convert.ToString(dia) + ", '"
                                    // + Convert.ToString(codigo) + "', "
                                    + ConverterCodigo(codigo) + "', "
                                    + Convert.ToString(nro) + ", "
                                    + Convert.ToString(atendimentos[mes, dia, codigo, nro]) + ", "
                                    + Convert.ToString(fornecimentos[mes, dia, codigo, nro])
                                    + " )";
                                    SqlCommand InserirDWDM = new SqlCommand(sqlDWDM, conexao4);

                                    InserirDWDM.ExecuteNonQuery();
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao gravar atendimentos diários. {0}", ex.Message));
            }
            finally
            {
                conexao4.Close();
                conexao4.Dispose();
            }
        }

        static void InicializaVetorDescricao()
        {
            try
            {
                for (int contador = 0; contador < 40; contador++)
                {
                    descricaoCodigo[contador] = "Não Cadastrado";
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao inicializar vetor descrição. {0}", ex.Message));
            }
        }

        static void CarregaVetorDescricao()
        {
            int uniformeCodigo = 0;
            String uniformeDescricao = "";

            // Conectar o Banco de Dados
            SqlConnection conexao = new SqlConnection(stringDeConexao);
            SqlDataReader rdrDescricao = null;

            try
            {
                conexao.Open();

                String sqlDescricao = "";
                sqlDescricao = "SELECT UniformeCodigo, "

                + " UniformeDescricao FROM UniformesDsc "
                + "ORDER BY UniformeCodigo";
                SqlCommand cmdSQLDsc = new SqlCommand(sqlDescricao, conexao);
                rdrDescricao = cmdSQLDsc.ExecuteReader();

                while (rdrDescricao.Read())
                {
                    uniformeCodigo =
                    Convert.ToInt32(rdrDescricao["UniformeCodigo"].ToString());
                    uniformeDescricao = rdrDescricao["UniformeDescricao"].ToString();

                    descricaoCodigo[uniformeCodigo] = uniformeDescricao;
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao carregar vetor. {0}", ex.Message));
            }
            finally
            {
                if (rdrDescricao != null)
                {
                    rdrDescricao.Close();
                    rdrDescricao.Dispose();
                }

                conexao.Close();
                conexao.Dispose();
            }
        }

        static String ConverterCodigo(int codigo)
        {
            String descricao = "";

            try
            {
                descricao = descricaoCodigo[codigo];
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao converter código. {0}", ex.Message));
            }

            return descricao;
        }

        static void Main(string[] args)
        {
            String sqlEmpresa = "";
            String sqlFilial = "";
            String sqlAno = "";

            SqlConnection conexao = new SqlConnection(stringDeConexao);
            SqlConnection conexao1 = new SqlConnection(stringDeConexao);
            SqlConnection conexao2 = new SqlConnection(stringDeConexao);

            try
            {
                conexao.Open();
                conexao1.Open();
                conexao2.Open();

                //-----------------------------------------
                // Inicializar e Carregar o Vetor Descrição
                //-----------------------------------------
                InicializaVetorDescricao();
                CarregaVetorDescricao();

                //------------------------------------
                // Escolher a Empresa a Ser Processada
                //------------------------------------
                sqlEmpresa = "SELECT DISTINCT EmpresaCodigo "
                + " FROM Movimentacao "
                + "ORDER BY EmpresaCodigo";
                SqlCommand cmdSQLEmp = new SqlCommand(sqlEmpresa, conexao);
                SqlDataReader rdrEmpresa = cmdSQLEmp.ExecuteReader();
                while (rdrEmpresa.Read())
                {
                    empresa = Convert.ToInt32(rdrEmpresa["EmpresaCodigo"].ToString());
                    //-----------------------------------
                    // Escolher a Filial a ser Processada
                    //-----------------------------------
                    sqlFilial = "SELECT DISTINCT FilialCodigo "
                    + " FROM Movimentacao "

                    + " WHERE EmpresaCodigo = " + empresa
                    + " ORDER BY FilialCodigo";
                    SqlCommand cmdSQLFil = new SqlCommand(sqlFilial, conexao1);
                    SqlDataReader rdrFilial = cmdSQLFil.ExecuteReader();
                    while (rdrFilial.Read())
                    {
                        filial = Convert.ToInt32(rdrFilial["FilialCodigo"].ToString());
                        //--------------------------------
                        // Escolher o Ano a ser Processado
                        //--------------------------------
                        sqlAno = "SELECT DISTINCT YEAR(MovimentacaoData) Ano "
                        + " FROM Movimentacao "
                        + " WHERE EmpresaCodigo = " + empresa
                        + " AND FilialCodigo = " + filial
                        + " ORDER BY Ano";
                        SqlCommand cmdSQLAno = new SqlCommand(sqlAno, conexao2);
                        SqlDataReader rdrAno = cmdSQLAno.ExecuteReader();

                        InicializarVetores();
                        while (rdrAno.Read())
                        {
                            ano = Convert.ToInt32(rdrAno["Ano"].ToString());

                            CarregarOsVetores();

                            GravarDWDMAtendimentosDiarios();

                            InicializarVetores();
                        }
                        rdrAno.Close();
                        rdrAno.Dispose();
                    }
                    rdrFilial.Close();
                    rdrFilial.Dispose();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Falha ao executar o programa. {0}", ex.Message));
            }
            finally
            {
                Console.WriteLine("acabou o programa");
                Console.ReadLine();
            }

        }
    }
}
