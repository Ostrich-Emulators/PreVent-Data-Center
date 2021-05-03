/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ostrichemulators.prevent.datastore;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ryan
 */
public class DatastoreException extends Exception {

  public DatastoreException() {
  }

  public DatastoreException( String message ) {
    super( message );
  }

  public DatastoreException( String message, Throwable cause ) {
    super( message, cause );
  }

  public DatastoreException( Throwable cause ) {
    super( cause );
  }

  public DatastoreException( String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace ) {
    super( message, cause, enableSuppression, writableStackTrace );
  }

}
