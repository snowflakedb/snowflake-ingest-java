package net.snowflake.ingest.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.NoSuchElementException;


/**
 * @author obabarinsa
 *
 * This class implements a generic ring buffer class which we can use
 * to keep track of a finite amount of data.
 *
 * For the ingest service, this is mostly used for window
 */
public class RingBuffer<E> extends AbstractCollection<E> implements Queue<E>
{
  //LOGGER for this class
  private static final Logger LOGGER = LoggerFactory.getLogger(RingBuffer.class.getName());

  //the underlying array of objects where we'll store data in this ring buffer
  private final E[] buffer;

  //the maximum capacity of this buffer
  private int capacity;

  //the current read pointer
  private int readIdx;

  //the current write index
  private int writeIdx;

  //the number of elements we for the reader to consume
  private int occupied;

  /**
   * Constructs a ring buffer with a specified size
   * @param capacity the maximum capacity of this buff
   * @throws IllegalArgumentException if capacity is less than 1
   */
   @SuppressWarnings("unchecked")
  public RingBuffer(int capacity)
  {
    LOGGER.info("Constructing RingBuffer with capacity {}", capacity);
    //if the capacity is less than 1, throw
    if(capacity < 1)
    {
      throw new IllegalArgumentException();
    }

    //instantiate the underlying buffer arraying

    buffer = (E[]) new Object[capacity];

    //copy the capacity
    this.capacity = capacity;

    //set the read and write indices
    readIdx = 0;
    writeIdx = 0;

    //we also start off with no items for a reader to consume
    occupied = 0;
  }


  /**
   * Returns the next valid position into the underlying array
   * - wraps if the we are at the end of the logical array
   * @param index the index for which we need a successor
   */

  private int getNextIndex(int index)
  {
    return (index + 1) % capacity;
  }


  /**
   * offer - attempts to add an object to the queue,
   * if the queue is at capacity, return false
   * @param obj what we're trying to append to the queue
   * @return whether or not we added the object to the queue
   */
  public boolean offer(E obj)
  {
    if(capacity == occupied)
    {
      return false;
    }

    //we have space for this object, so insert
    buffer[writeIdx] = obj;
    //update our write pointer and occupied count
    writeIdx = getNextIndex(writeIdx);
    occupied++;
    return true;
  }


  /**
   * add - appends an element to the end of this queue
   * @param obj - the object we're trying to add to the queue
   * @return always returns true if it doesn't throw
   * @throws IllegalStateException if we are at capacity
   */
  public boolean add(E obj)
  {
    //check to see if we're at capacity
    if(capacity == occupied)
    {
      throw new IllegalStateException();
    }

    //we have enough space to insert the object, so insert
    buffer[writeIdx] = obj;
    //update our metadata (write pointer and our occupied count)
    writeIdx = getNextIndex(writeIdx);
    occupied++;
    return true;
  }


  /**
   * peek - Returns the element currently under the read pointer
   * Does NOT update the read position
   * If there is no valid element, return null
   */  public E peek()
  {
    if(occupied == 0)
    {
      return null;
    }
    return buffer[readIdx];
  }

  /**
   * element Returns the element current under the read pointer
   * Does NOT update the read position
   * @throws IllegalStateException if we have no valid readable objects
   */

  public E element()
  {
    if(occupied == 0)
    {
      throw new IllegalStateException();
    }
    return buffer[readIdx];
  }


  /**
   * remove - returns the current head of this queue and removes it
   * This *does* update the read position
   * @return the object which was formerly the head of this queue
   * @throws NoSuchElementException if we have no items to be consumed
   */
  public E remove()
  {
    if(occupied == 0)
    {
      throw new NoSuchElementException();
    }
    //grab the head of the queue
    E ret = peek();
    //calculate the next read index
    readIdx = getNextIndex(readIdx);
    //decrement the number of objects we're storing
    occupied--;
    return ret;
  }


  /**
   * poll - returns the head of this queue and removes it from the queue.
   * If no element exists returns null
   * @return the former head of this queue
   */
  public E poll()
  {
    if(occupied == 0)
    {
      return null;
    }
    //grab the head of the queue
    E ret = peek();
    //update our read counter
    readIdx = getNextIndex(readIdx);
    //decrement our occupied counter
    occupied--;
    return ret;
  }

  /**
   * isEmpty - are there any items we can get from this queue?
   * @return whether or not there are any consumable items in this buffer
   */
  public boolean isEmpty()
  {
    return occupied == 0;
  }

  /**
   * getCapacity - gives back the capacity of this buffer
   * @return the capacity of this buffer
   */
  public int getCapacity()
  {
    return capacity;
  }

  /**
   * size - returns the number of occupied slots in this queue
   * @return the number of slots currently in use in this buffer
   */
  public int size()
  {
    return occupied;
  }

  /**
   * clear - effectively empties the array by resetting
   * all of our pointers and clearing our buffer
   */
  public void clear()
  {
    //reset our indicies to 0
    readIdx = 0;
    writeIdx = 0;
    //clear out the occupied counter
    occupied = 0;
    //to make this code a bit safer, clear the junk objects
    Arrays.fill(buffer, null);
  }

  /**
   * toArray - builds an array containing all objects in this queue
   * @return an array containing all elements of this queue
   * @throws ArrayStoreException - if the element
   */
  public Object[] toArray()
  {
    //allocate a new array for our objects
    Object[] ret = new Object[occupied];

    //copy our read pointer
    int needle = readIdx;
    //for each occupied slot ...
    for(int i = 0; i < occupied; i++)
    {
      //copy over a reference to the object
      ret[i] = buffer[needle];
      //update our needle
      needle = getNextIndex(needle);
    }

    return ret;
  }


  /**
   * contains - checks whether or not an object is in this queue
   * @param obj the needle we're searching for in the queue
   * @return did we find an object equal to this one in the queue
   */
  public boolean contains(Object obj)
  {
    //copy our read pointer
    int needle = readIdx;
    //for each object in our queue ..
    for(int i = 0; i < occupied; i++)
    {
      //if both are null, return true
      if(obj == null && buffer[needle] == null)
      {
        return true;
      }
      //if both aren't null and they are equal, return true
      else if(obj != null && buffer[needle] != null && obj.equals(buffer[needle]))
      {
        return true;
      }

      //update our read index
      needle = getNextIndex(needle);
    }

    //we didn't find it
    return false;
  }

  /**
   * iterator - returns an iterator that allows you to view all
   * elements in this queue
   * This iterator does NOT allow for remove()
   * @return an instance of ring iterator
   */

  public Iterator<E> iterator()
  {
    return new RingIterator(this);
  }

  /**
   * This class implements an iterator for RingBuffer
   * It does not support remove and requires a handle for the creating RingBuffer
   * @author obabarinsa
   */
  private class RingIterator implements Iterator<E> {

    //the count of how many elements we've used of those available
    private int usedElems;

    //a pointer back to the RingBuffer which owns this iterator
    RingBuffer<E> ringBuffer;

    //our own index into the ringbuffers array
    private int idx;

    /**
     * A constructor for our RingBuffer instance
     * @param buffer the constructing ring buffer from which we'll get values
     */
    RingIterator(RingBuffer<E> buffer)
    {
      //we haven't used any elements of yet
      usedElems = 0;

      //preserve a link to the ringbuffer
      ringBuffer = buffer;

      //get a handle the read index
      idx = buffer.readIdx;
    }

    /**
     * remove - not supported and will just throw an exception
     * @throws UnsupportedOperationException will ALWAYS throw this exception
     */
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    /**
     * hasNext - returns whether our not there are still unvisited
     * elements in this queue
     * @return do we have more of the queue to explore?
     */
    public boolean hasNext()
    {
      return usedElems < ringBuffer.occupied;
    }

    /**
     * next - attempts to return the next available item in the queue
     * @throws NoSuchElementException thrown if we have exhausted the queue
     */
    public E next()
    {
      //if we've used up the queue, throw an exception
      if(usedElems >= ringBuffer.occupied)
      {
        throw new NoSuchElementException();
      }
      //otherwise fish out the appropriate element
      E ret = ringBuffer.buffer[idx];

      //update all our metadata
      usedElems++;
      idx = ringBuffer.getNextIndex(idx);

      return ret;
    }


  }

}
