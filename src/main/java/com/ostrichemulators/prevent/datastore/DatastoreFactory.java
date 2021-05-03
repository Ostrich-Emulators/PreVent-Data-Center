/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent.datastore;

import com.ostrichemulators.prevent.Worklist;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ryan
 */
public class DatastoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger( DatastoreFactory.class );

  public static Database open( Path p ) throws DatastoreException {
    try {
      Class.forName( "org.apache.derby.jdbc.EmbeddedDriver" ).getDeclaredConstructor().newInstance();
      Connection conn = DriverManager.getConnection( "jdbc:derby:" + p.toAbsolutePath() + ";create=true" );
      DerbyDatabase.checkAndCreate( conn );

      return new DerbyDatabase( conn );
    }
    catch ( Exception x ) {
      throw new DatastoreException( x );
    }
  }
}
