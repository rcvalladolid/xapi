/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.utpl.xapi.conexion;

import java.sql.*;

/**
 *
 * @author rcvalladolid
 */
public class ConexionEva {

    private Connection con;

    public Connection conectarMysql() {
        try {

            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://utpl-new.c17ceauzhwet.us-east-1.rds.amazonaws.com/eva3", "landacay", "xsw234*");

        } catch (ClassNotFoundException | SQLException e) {            
            System.out.println("SQLException: " + e.getMessage());
        }
        return con;
    }

    public void desconectarMysql() {
        try {
            con.close();
        } catch (SQLException ex) {
            ex.getMessage();
        }
    }
}
