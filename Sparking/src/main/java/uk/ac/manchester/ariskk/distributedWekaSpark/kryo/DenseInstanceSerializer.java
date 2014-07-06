package uk.ac.manchester.ariskk.distributedWekaSpark.kryo;

import weka.core.DenseInstance;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DenseInstanceSerializer  extends Serializer<DenseInstance> {

	class Accessor extends DenseInstance
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public Accessor(DenseInstance parent)
		{
			super(parent);
		}

		public double[] getValues()
		{
			return m_AttValues;
		}
		public double getWeight()
		{
			return m_Weight;
		}
	}
	@Override
	public void write(Kryo kryo, Output output, DenseInstance object) {
		Accessor accesor = new Accessor(object);

		output.writeDouble(accesor.getWeight());
		kryo.writeObjectOrNull(output, accesor.getValues(),double[].class);
		//kryo.writeObjectOrNull(output, object.dataset(), Instances.class);
	}	

	@Override
	public DenseInstance read(Kryo kryo, Input input, Class<DenseInstance> type) {

		double weight = input.readDouble();
		double[] values =kryo.readObjectOrNull(input, double[].class);
		//Instances header = kryo.readObjectOrNull(input, Instances.class) ;
		DenseInstance newInstance = new DenseInstance(weight, values);
		//newInstance.setDataset(header);
		return newInstance;
	}



}
