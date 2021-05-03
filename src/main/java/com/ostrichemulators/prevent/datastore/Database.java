/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ostrichemulators.prevent.datastore;

import com.ostrichemulators.prevent.WorkItem;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author ryan
 */
public interface Database extends AutoCloseable {

  public List<WorkItem> getAllWorkItems() throws DatastoreException;

  /**
   * Upserts the given items.
   * @param items
   * @return the newly-inserted (not updated) items
   * @throws DatastoreException
   */
  public Collection<WorkItem> upsert( Collection<WorkItem> items ) throws DatastoreException;

}
