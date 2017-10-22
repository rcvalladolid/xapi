/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.utpl.xapi.main;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import ec.edu.utpl.xapi.logica.LogicaAnunciosXapi;
import java.util.Arrays;

/**
 *
 * @author rcvalladolid
 */
public class Main {

    public static void main(String[] args) {

        /*try {

            String user = "eva";        // the user name
            String database = "aprendizaje";    // the name of the database in which the user is defined
            char[] password = "eva.eva".toCharArray();    // the password as a character array

            MongoCredential credenciales = MongoCredential.createCredential(user, database, password);

            ServerAddress serverAddress = new ServerAddress("localhost", 27017);
            MongoClient mongoCliente = new MongoClient(serverAddress, Arrays.asList(credenciales));

            //Base de datos llamada aprendizaje
            DB db_moongo = mongoCliente.getDB("aprendizaje");

            System.out.println("BasicDBObject example...");                        
            
            //DBCollection dBCollection = db_moongo.createCollection("prueba", document);
            //Tabla o collección llamada prueba
            DBCollection dbCollection = db_moongo.getCollection("prueba");
            
            BasicDBObject document = new BasicDBObject();
            document.put("database", "aprendizaje");
            document.put("table", "prueba");
            dbCollection.insert(document);

            System.out.println("******************************************************************");
            System.out.println("ESTADO: " + db_moongo.getStats());
            System.out.println("OPCION: " + db_moongo.getOptions());

        } catch (MongoException e) {
            e.printStackTrace();
        }*/
        
        int pdoid = 31;
        
        try {
            
            LogicaAnunciosXapi logicaAnunciosXapi = new LogicaAnunciosXapi();
            logicaAnunciosXapi.obtener_anuncios_xapi(pdoid);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
}
