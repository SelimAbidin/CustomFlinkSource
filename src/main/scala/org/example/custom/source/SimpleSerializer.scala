package org.example.custom.source

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.flink.core.io.SimpleVersionedSerializer

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}

class SimpleSerializer[T] extends SimpleVersionedSerializer[T]{
  override def getVersion: Int = 1

  override def serialize(obj: T): Array[Byte] = {
    val boas:ByteArrayOutputStream = new ByteArrayOutputStream();
    val oos:ObjectOutputStream = new ObjectOutputStream(boas)
    oos.writeObject(obj)
    boas.toByteArray
  }

  override def deserialize(version: Int, serialized: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(serialized)
    val ois: ObjectInputStream = new ObjectInputStream(bais)
    ois.readObject.asInstanceOf[T]
  }
}
