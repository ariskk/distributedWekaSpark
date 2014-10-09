package uk.ac.manchester.ariskk.distributedWekaSpark.kryo;

import java.util.Properties;

import weka.core.ProtectedProperties;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

public class ProtectedPropertiesSerializer extends FieldSerializer<ProtectedProperties>{

	@Override
	protected ProtectedProperties create(Kryo kryo, Input input,
			Class<ProtectedProperties> type) {
		return new ProtectedProperties(new Properties());
	}

	@Override
	protected ProtectedProperties createCopy(Kryo kryo,
			ProtectedProperties original) {
		// TODO Auto-generated method stub
		return (ProtectedProperties) original.clone();
	}

	public ProtectedPropertiesSerializer(Kryo kryo, Class<?> type) {
		super(kryo, type);
		// TODO Auto-generated constructor stub
	}

}
