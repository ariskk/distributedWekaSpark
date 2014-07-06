package uk.ac.manchester.ariskk.distributedWekaSpark.kryo;

import java.util.ArrayList;

import weka.core.Attribute;
import weka.core.Instances;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class InstancesSerializer extends Serializer<Instances>{

	@Override
	public void write(Kryo kryo, Output output, Instances object) {
		String relationName = object.relationName();
		int classIndex = object.classIndex();
		int numAttributes = object.numAttributes();
		output.writeString(relationName);
		output.writeInt(classIndex);
		output.writeInt(numAttributes);
		for (int i = 0; i < numAttributes; i++) 
			kryo.writeObjectOrNull(output, object.attribute(i), Attribute.class);
	}

	@Override
	public Instances read(Kryo kryo, Input input, Class<Instances> type) {

		String relationName = input.readString();
		int classIndex = input.readInt();
		int numAttributes = input.readInt();
		ArrayList<Attribute> attInfo = new ArrayList<Attribute>();
		for (int i =0 ;i < numAttributes; i++)
			attInfo.add( kryo.readObjectOrNull(input, Attribute.class));
		Instances ret = new Instances( relationName, attInfo, 0);
		ret.setClassIndex(classIndex);
		return ret;
	}


}