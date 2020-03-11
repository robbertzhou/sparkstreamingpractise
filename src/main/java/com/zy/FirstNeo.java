package com.zy;

import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;

public class FirstNeo {
    public static void main(String[] args){
        Driver driver = GraphDatabase.driver( "bolt://192.168.0.126:7687", AuthTokens.basic( "neo4j", "neo4j" ) );
        Session session = driver.session();
        session.run( "CREATE (a:Person {name: {name}, title: {title}})",
                parameters( "name", "Arthur001", "title", "King001" ) );

        StatementResult result = session.run( "MATCH (a:Person) return a",
                parameters( "name", "Arthur001" ) );
        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "title" ).asString() + " " + record.get( "name" ).asString() );
        }
        session.close();
        driver.close();
    }
}
