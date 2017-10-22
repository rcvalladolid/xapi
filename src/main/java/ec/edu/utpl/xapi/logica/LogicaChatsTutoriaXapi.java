/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ec.edu.utpl.xapi.logica;

import ec.edu.utpl.xapi.conexion.ConexionEva;
import com.mongodb.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 *
 * @author rcvalladolid
 */
public class LogicaChatsTutoriaXapi {

    ConexionEva cmysql = new ConexionEva();
    //ConexionOracle coracle = new ConexionOracle();

    public LogicaChatsTutoriaXapi() {

    }

    public boolean obtener_anuncios_xapi(int pdoid) {

        int total_anuncios = 0;

        Statement stmt_mysql = null;
        ResultSet rs_mysql = null;
        Connection con_mysql = cmysql.conectarMysql();

        String user = "eva";
        String database = "aprendizaje";
        char[] password = "eva.eva".toCharArray();

        MongoCredential credenciales = MongoCredential.createCredential(user, database, password);

        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
        MongoClient mongoCliente = new MongoClient(serverAddress, Arrays.asList(credenciales));

        DB db = mongoCliente.getDB("aprendizaje");

        System.out.println("**********************************************");
        System.out.println("Seleccionando la base de datos: aprendizaje");
        DBCollection collection = db.getCollection("eva");
        System.out.println("Seleccionando la Collection: eva");
        System.out.println("**********************************************");       

        Date date = new Date();
        DateFormat hourdateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        String cdc_fecha = hourdateFormat.format(date);

        try {

            stmt_mysql = cmysql.conectarMysql().createStatement();

            String contar = "SELECT COUNT(DISTINCT man.objectid) as TOTAL "
                    + "FROM "
                    + "( "
                    + "SELECT DISTINCT "
                    + "		sl.eventname "
                    + "		,sl.action "
                    + "		,sl.target "
                    + "		,sl.objecttable "
                    + "		,sl.objectid "
                    + "		,sl.crud "
                    + "		,sl.userid "
                    + "		,sl.courseid "
                    + "		,MAX(DATE_SUB(FROM_UNIXTIME(sl.timecreated), INTERVAL 5 HOUR)) AS timecreated "
                    + "FROM mdl_logstore_standard_log sl "
                    + "	,mdl_context ct "
                    + "	,mdl_role_assignments ra "
                    + "	,mdl_nsacademic_parallel pa "
                    + "	,mdl_nsacademic_component co "
                    + "	,mdl_nsacademic_period pe "
                    + "WHERE sl.courseid = ct.instanceid "
                    + "AND ct.contextlevel = 50 "
                    + "AND ct.id = ra.contextid "
                    + "AND sl.userid	= ra.userid "
                    + "AND ra.roleid = 3 "
                    + "AND sl.courseid = pa.courseid "
                    + "AND pa.componentid = co.id  "
                    + "AND co.periodid = pe.id "
                    + "AND pe.id = '" + pdoid + "' "
                    //+ "AND sl.courseid = 29457 "
                    + "AND sl.objecttable LIKE 'forum_discussions' "
                    + "AND sl.crud IN ('c') "
                    + "GROUP BY sl.eventname "
                    + "		,sl.action "
                    + "		,sl.target "
                    + "		,sl.objecttable "
                    + "		,sl.objectid "
                    + "		,sl.crud "
                    + "		,sl.userid "
                    + "		,sl.courseid "
                    + "ORDER BY sl.id ASC "
                    + ") man "
                    + "	,mdl_forum_posts fp "
                    + "	,mdl_forum_discussions fd "
                    + "	,mdl_forum fo "
                    + "	,mdl_course_modules cm	"
                    + "	,mdl_nsacademic_parallel par "
                    + "	,mdl_nsacademic_component com "
                    + "	,mdl_user us "
                    + "WHERE man.objectid = fd.id "
                    + "AND fp.discussion = fd.id "
                    + "AND fd.forum = fo.id "
                    + "AND fo.course = fd.course "
                    + "AND man.courseid = fo.course "
                    + "AND cm.course = fo.course "
                    + "AND cm.instance = fo.id "
                    + "AND cm.idnumber NOT IN ('FOR_1B', 'FOR_2B') "
                    + "AND man.courseid = par.courseid "
                    + "AND par.componentid = com.id "
                    + "AND fp.subject NOT LIKE 'Re:%' "
                    + "AND man.userid = us.id ";

            rs_mysql = stmt_mysql.executeQuery(contar);

            while (rs_mysql.next()) {
                total_anuncios = rs_mysql.getInt("TOTAL");
            }

            System.out.println("-----------------------------------------------------------------");
            System.out.println("TOTAL REGISTROS DE ANUNCIOS XAPI: " + total_anuncios);
            System.out.println("-----------------------------------------------------------------");

            String sql = "SELECT DISTINCT man.eventname AS evento_eva "
                    + "		,man.action AS accion_eva "
                    + "		,man.target AS target_eva "
                    + "		,man.objecttable AS objettable_eva "
                    + "		,man.objectid AS objectid_eva "
                    + "		,man.crud AS crud_eva "
                    + "		,cm.idnumber AS codigo_anuncio_eva "
                    + "		,fp.subject AS 	nombre_anuncio_eva "
                    + "		,fp.message AS decripcion_anuncio_eva "
                    + "		,man.userid AS userid_eva "
                    + "		,us.idnumber AS identificacion_usuario_eva "
                    + "         ,us.username AS username_user_eva "
                    + "		,us.firstname AS nombre_usuario_eva "
                    + "		,us.lastname AS apellido_usuario_eva "
                    + "		,us.email AS mail_usuario_eva "
                    + "		,man.courseid AS curso_eva "
                    + "		,man.timecreated AS fecha_creacion_anuncio_eva "
                    + "		,com.wscode AS codigo_curso_eva	"
                    + "		,com.wsname AS 	 nombre_curso_eva "
                    + "		,par.wsname AS paralelo_eva "
                    + "FROM "
                    + "( "
                    + "SELECT DISTINCT "
                    + "		sl.eventname "
                    + "		,sl.action "
                    + "		,sl.target "
                    + "		,sl.objecttable "
                    + "		,sl.objectid "
                    + "		,sl.crud "
                    + "		,sl.userid "
                    + "		,sl.courseid "
                    + "		,MAX(DATE_SUB(FROM_UNIXTIME(sl.timecreated), INTERVAL 5 HOUR)) AS timecreated "
                    + "FROM mdl_logstore_standard_log sl "
                    + "	,mdl_context ct "
                    + "	,mdl_role_assignments ra "
                    + "	,mdl_nsacademic_parallel pa "
                    + "	,mdl_nsacademic_component co "
                    + "	,mdl_nsacademic_period pe "
                    + "WHERE sl.courseid = ct.instanceid "
                    + "AND ct.contextlevel = 50 "
                    + "AND ct.id = ra.contextid "
                    + "AND sl.userid = ra.userid "
                    + "AND ra.roleid = 3 "
                    + "AND sl.courseid = pa.courseid "
                    + "AND pa.componentid = co.id "
                    + "AND co.periodid = pe.id "
                    + "AND pe.id = '" + pdoid + "' "
                    //+ "AND sl.courseid = 29457 "
                    + "AND sl.objecttable LIKE 'forum_discussions' "
                    + "AND sl.crud IN ('c') "
                    + "GROUP BY sl.eventname "
                    + "		,sl.action "
                    + "		,sl.target "
                    + "		,sl.objecttable "
                    + "		,sl.objectid "
                    + "		,sl.crud "
                    + "		,sl.userid "
                    + "		,sl.courseid "
                    + "ORDER BY sl.id ASC "
                    + ") man "
                    + "	,mdl_forum_posts fp "
                    + "	,mdl_forum_discussions fd "
                    + "	,mdl_forum fo "
                    + "	,mdl_course_modules cm	"
                    + "	,mdl_nsacademic_parallel par "
                    + "	,mdl_nsacademic_component com "
                    + "	,mdl_user us "
                    + "WHERE man.objectid = fd.id "
                    + "AND fp.discussion = fd.id "
                    + "AND fd.forum = fo.id "
                    + "AND fo.course = fd.course "
                    + "AND man.courseid = fo.course "
                    + "AND cm.course = fo.course "
                    + "AND cm.instance = fo.id "
                    + "AND cm.idnumber NOT IN ('FOR_1B', 'FOR_2B') "
                    + "AND man.courseid = par.courseid "
                    + "AND par.componentid = com.id "
                    + "AND fp.subject NOT LIKE 'Re:%' "
                    + "AND man.userid = us.id ";

            rs_mysql = stmt_mysql.executeQuery(sql);
            int i = 0;
            while (rs_mysql.next()) {

                String evento_eva = rs_mysql.getString(1);
                String accion_eva = rs_mysql.getString(2);
                String target_eva = rs_mysql.getString(3);
                String objettable_eva = rs_mysql.getString(4);
                String objectid_eva = rs_mysql.getString(5);
                //String crud_eva = rs_mysql.getString(6);
                //String codigo_anuncio_eva = rs_mysql.getString(7);
                String nombre_anuncio_eva = rs_mysql.getString(8);
                String decripcion_anuncio_eva = rs_mysql.getString(9);
                String userid_eva = rs_mysql.getString(10);
                String identificacion_usuario_eva = rs_mysql.getString(11);
                String username_user_eva = rs_mysql.getString(12);
                String nombre_usuario_eva = rs_mysql.getString(13);
                String apellido_usuario_eva = rs_mysql.getString(14);
                String mail_usuario_eva = rs_mysql.getString(15);
                String curso_eva = rs_mysql.getString(16);
                String fecha_creacion_anuncio_eva = rs_mysql.getString(17);
                String codigo_curso_eva = rs_mysql.getString(18);
                String nombre_curso_eva = rs_mysql.getString(19);
                String paralelo_eva = rs_mysql.getString(20);

                
                System.out.println("***************************************************************************");
                System.out.println("CURSO: " + curso_eva + " ANUNCIOS: " + objectid_eva);
                System.out.println("***************************************************************************");
                
                try {
                    
                    BasicDBObjectBuilder documento_raiz = BasicDBObjectBuilder.start();
                    
                    //collection.remove(new BasicDBObject());

                    /**
                     * ********************* ACTOR *************************
                     */
                    BasicDBObjectBuilder actor_account = BasicDBObjectBuilder.start()
                            .add("homePage", "https://eva3.utpl.edu.ec/login/index.php")                            
                            .add("name", username_user_eva)
                            .add("role", "teacher");

                    BasicDBObjectBuilder actor = BasicDBObjectBuilder.start()
                            .add("ObjectType", "Agent")
                            .add("id", userid_eva)
                            .add("name", apellido_usuario_eva + " " + nombre_usuario_eva)
                            .add("mbox", mail_usuario_eva)
                            .add("account", actor_account.get());

                    /**
                     * ********************* VERB *************************
                     */
                    BasicDBObjectBuilder verb_display = BasicDBObjectBuilder.start()
                            .add("en-US", accion_eva);

                    BasicDBObjectBuilder verb = BasicDBObjectBuilder.start()
                            .add("id", "http://activitystrea.ms/schema/1.0/create")
                            .add("display", verb_display.get());

                    /**
                     * ********************* OBJECT *************************
                     */
                    BasicDBObjectBuilder object_definition_name = BasicDBObjectBuilder.start()
                            .add("en-ES", nombre_anuncio_eva);

                    BasicDBObjectBuilder object_definition_description = BasicDBObjectBuilder.start()
                            .add("en-ES", decripcion_anuncio_eva);

                    BasicDBObjectBuilder object_definition_extensions = BasicDBObjectBuilder.start()
                            .add("course", curso_eva)
                            .add("code", codigo_curso_eva)
                            .add("name", nombre_curso_eva)
                            .add("parallel", paralelo_eva);

                    BasicDBObjectBuilder object_definition = BasicDBObjectBuilder.start()
                            .add("type", "http://id.tincanapi.com/activitytype/"+target_eva)
                            .add("name", object_definition_name.get())
                            .add("description", object_definition_description.get())
                            .add("extensions", object_definition_extensions.get());

                    BasicDBObjectBuilder object = BasicDBObjectBuilder.start()
                            .add("objectType", "Activity")
                            .add("id", "https://eva3.utpl.edu.ec/mod/forum/discuss.php?d="+objectid_eva)
                            .add("definition", object_definition.get());

                    /**
                     * ********************* AUTHORITY *************************
                     */
                    
                    /*BasicDBObjectBuilder authority = BasicDBObjectBuilder.start()
                            .add("name", "Roberto Valladolid")
                            .add("mbox", "mailto:rcvalladolid@utpl.edu.ec");*/
                    
                    /**
                     * *******************************************************************
                     */
                                        
                    documento_raiz.add("anuncioId", objectid_eva);
                    documento_raiz.add("timestamp", fecha_creacion_anuncio_eva);
                    documento_raiz.add("actor", actor.get());
                    documento_raiz.add("verb", verb.get());
                    documento_raiz.add("object", object.get());
                    documento_raiz.add("stored", cdc_fecha);
                    //documento_raiz.add("authority", authority);
                    
                    collection.insert(documento_raiz.get());

                } catch (MongoException e) {
                    System.err.println("Ecepción insertar anuncios xapi: " + e.getMessage());
                    e.printStackTrace();
                }
                i++;
                
                
            }

            
            

            
        } catch (SQLException e) {
            System.err.println("SQLEcepción SELECT MYSQL: " + e.getMessage());
        } finally {
            if (stmt_mysql != null) {
                try {
                    stmt_mysql.close();
                    rs_mysql.close();
                } catch (Exception e) {
                    System.err.println("Ecepción CERRAR STATEMENT MYSQL: " + e.getMessage());
                }
            }
            if (con_mysql != null) {
                try {
                    con_mysql.close();
                    cmysql.desconectarMysql();
                } catch (Exception e) {
                    System.err.println("Ecepción CERRAR CONEXIÓN MYSQL: " + e.getMessage());
                }
            }
        }
        return true;
    }
}
