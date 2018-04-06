package com.pingidentity.sync.pipe;

/**
 * This a very simple interface to allow the extension to generically
 * process a queue of operations implementing this interface
 * Different strategies may be implemented to better address specific cases 
 */
public interface DereferenceOperation {
  public void execute();
}
