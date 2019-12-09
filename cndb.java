package conexion;

import holamundo.console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
// MSSQL  SERVER
import java.sql.DatabaseMetaData;

public class cndb {
    private Connection connection = null;
    private ResultSet rs, rs2 = null;
    private Statement s, sql = null;
    
    public String[] Result = null;
    public String[] SResult = null;
    
    public static Connection connObj;
    public static String JDBC_URL = "jdbc:sqlserver://10.10.100.18:1433;databaseName=VAD20";
    
    // POSTGRESQL
    public boolean connect(String pro, int org) {
        if(connection != null) return false;        
        
        String url = "jdbc:postgresql://172.30.143.118:5432/idempiere";
        String user = "adempiere";
        String pass = "4d3mp13r3*";
        try {
            Class.forName("org.postgresql.Driver");
            
            connection = DriverManager.getConnection(url, user, pass);
            
            if(connection != null) {
                console.log("Conectando a Base de Datos...");
                if(Select(pro, org)) console.log("Ejecutado...");
                //mssql_connect(pro, org);
            }
        } catch (Exception e) {
            console.log("Error de conexion.");
            return false;
        }
        
        return true;
    }
    
    // MSSQL SERVER
    public boolean mssql_connect(String pro, int org) {
        if(connObj != null) {
            if(mssql_query(pro, org)) console.log("Ejecutado...");
        }      
       
        String user = "sa";
        String pass = "";
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            
            connObj = DriverManager.getConnection(JDBC_URL, user, pass);
            
            if(connObj != null) {
                //DatabaseMetaData metaObj = (DatabaseMetaData) connObj.getMetaData();
                //console.log("Driver Name?= " + metaObj.getDriverName() + ", Driver Version?= " + connObj);
                console.log("Conectando a Base de Datos...");
                if(mssql_query(pro, org)) console.log("Ejecutado...");
            }
        } catch (Exception e) {
            console.log("Error de conexion.");
            return false;
        }
        
        return true;
    }
    
    /*public String addComas(String nStr){
        nStr += "";
        String[] x = nStr.split(".");
        String x1 = x[0];
        String x2 = x.length > 1 ? "." + x[1] : "";
        
    }*/
    
    public boolean Select(String pro, int org)
    {
        try 
        {
            s = connection.createStatement();
            console.log("Producto filtrado: " + pro);
            rs = s.executeQuery("select org.ad_org_id, pro.sku, pro.name, valu.Value, pri.pricelist, ctx.name, (select (case when pro.C_TaxCategory_ID = '6000019' then (pri.pricelist * 1.08) when pro.C_TaxCategory_ID = '6000022' then (pri.pricelist * 1.16) else  (pri.pricelist) end )) as PRECIOVENTA, orgs.NAME from BSCA_ProductValue valu join M_Product pro on pro.M_Product_ID=valu.M_Product_ID left join C_TaxCategory ctx on ctx.C_TaxCategory_ID=pro.C_TaxCategory_ID left join BSCA_ProductOrg org on pro.M_Product_ID=org.M_Product_ID join M_ProductPrice pri on  pri.M_Product_ID=pro.M_Product_ID and pri.ad_org_id=org.ad_org_id join ad_org orgs on orgs.ad_org_id=org.ad_org_id where valu.value='"+pro+"' and org.ad_org_id="+org + " limit 1");
            DecimalFormat df2 = new DecimalFormat("##,###,##0.00");
            
            // SICLO 
            while(rs.next())
            {
                
                String sku = rs.getString("sku");
                String name = rs.getString("name");
                console.log(rs.getString("name"));
                Double price = Double.parseDouble(rs.getString("pricelist"));
                
                Result = new String[7];
                Result[0] = sku;
                Result[1] = name;
                Result[2] = price.toString();
                Result[3] = rs.getString(8);
                Result[4] = df2.format(rs.getDouble(7));
                if(rs.getLong(7) > rs.getLong(5))
                    Result[5] = "" + df2.format( (rs.getLong(7) - rs.getLong(5)) );
                else
                    Result[5] = "0.00";
                
                //Result[2] = value;
                //console.log(df2.format(rs.getDouble(7)));
                //console.log("ORG: " + rs.getString(8));
                //console.log(df2.format(rs.getDouble(5)) + " " + df2.format(rs.getDouble(7)) + " " + Result[5] + " \n Total Generado : " + "SKU: " + Result[0] + " Desc: " + Result[1] + " Precio: " + Result[2] + " PV: " + Result[4] + " IVA: " + Result[5]);
               
            }
        }
        catch(Exception e)
        {
            console.log("Error: " + e);
            return false;
        }
        
        return true;
        
    }
    
    public boolean mssql_query(String pro, int org)
    {
        try 
        {
            sql = connObj.createStatement();
            console.log("Producto filtrado SQL: " + pro);
            rs2 = sql.executeQuery("SELECT TOP(1) (CASE P.n_impuesto1 WHEN 16 THEN (P.n_precio1 * 1.16) WHEN 8 THEN (P.n_precio1 * 1.08) ELSe P.n_precio1 END) as precio FROM VAD20..MA_CODIGOS C INNER JOIN VAD20..MA_PRODUCTOS P ON  C.c_codnasa= P.C_CODIGO WHERE C.c_codnasa = '"+pro+"' OR C.c_codigo = '"+pro+"'");
            DecimalFormat df2 = new DecimalFormat("##,###,##0.00");
            
            // SICLO 
            while(rs2.next())
            {
                System.out.println(" probando todo esto " + rs2.getString("precio"));
                Double price = rs2.getDouble("precio");
                
                SResult = new String[1];
                //if("10528".equals(pro)) SResult[0] = df2.format(price) + 22;
                //else SResult[0] = df2.format(price);
                SResult[0] = df2.format(price);
            }
        }
        catch(Exception e)
        {
            console.log("Error: " + e);
            return false;
        }
        
        return true;
        
    }
    
    public String getDesc()
    {
        return Result[1];
    }
    
    public String getPrice()
    {
        return Result[2];
    }
    
    public String getSKU()
    {
        return Result[0];
    }
    
    public String getOrg()
    {
        return Result[3];
    }
    
    public String getPV()
    {
        return Result[4];
    }
    
    public String getIVA()
    {
        return Result[5];
    }   
    
    // SQL Result
    public String getStellar() {
        return SResult[0];
    }
    
}
