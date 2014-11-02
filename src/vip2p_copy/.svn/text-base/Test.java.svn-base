package vip2p_copy;

import java.util.*;

import org.apache.hadoop.hbase.thrift.generated.Hbase.isTableEnabled_args;

public class Test {

	public static void main(String[] args) throws VIP2PExecutionException {
		// TODO Auto-generated method stub
		TupleMetadataType[] types = new TupleMetadataType[2];
		types[0] = TupleMetadataType.STRING_TYPE;
		types[1] = TupleMetadataType.INTEGER_TYPE;
		String[] colNames = new String[2];
		colNames[0] = "col1";
		colNames[1] = "col2";
		NRSMD[] nestedChildren = new NRSMD[0];
		NRSMD nrsmd = new NRSMD(2, types, colNames, nestedChildren);

		int[] integerFields = new int[1];
		integerFields[0] = 1;

		char[][] stringFields = new char[1][];
		stringFields[0] = "t1".toCharArray();

		ArrayList<NTuple>[] ntuples = new ArrayList[0];
		NTuple nTuple = new NTuple(nrsmd, stringFields, integerFields, ntuples);

		char[][] stringFields2 = new char[1][];
		int[] integerFields2 = new int[1];
		stringFields2[0] = "t2".toCharArray();
		integerFields2[0] = 2;
		NTuple nTuple2 = new NTuple(nrsmd, stringFields2, integerFields2,
				ntuples);

		char[][] stringFields3 = new char[1][];
		stringFields3[0] = "t1".toCharArray();
		int[] integerFields3 = new int[1];
		integerFields3[0] = 3;

		NTuple nTuple3 = new NTuple(nrsmd, stringFields3, integerFields3,
				ntuples);

		ArrayList<NTuple> v = new ArrayList<NTuple>();
		v.add(nTuple);
		v.add(nTuple2);
		v.add(nTuple3);
		ArrayIterator arrayIterator = new ArrayIterator(v, nrsmd);

		int[] integerFields4 = new int[1];
		integerFields4[0] = 4;

		char[][] stringFields4 = new char[1][];
		stringFields4[0] = "t1".toCharArray();

		NTuple nTuple4 = new NTuple(nrsmd, stringFields4, integerFields4,
				ntuples);

		char[][] stringFields5 = new char[1][];
		int[] integerFields5 = new int[1];
		stringFields5[0] = "t2".toCharArray();
		integerFields5[0] = 5;
		NTuple nTuple5 = new NTuple(nrsmd, stringFields5, integerFields5,
				ntuples);

		char[][] stringFields6 = new char[1][];
		stringFields6[0] = "t4".toCharArray();
		int[] integerFields6 = new int[1];
		integerFields6[0] = 3;

		NTuple nTuple6 = new NTuple(nrsmd, stringFields6, integerFields6,
				ntuples);
		ArrayList<NTuple> v2 = new ArrayList<NTuple>();
		v2.add(nTuple4);
		v2.add(nTuple5);
		v2.add(nTuple6);

		ArrayIterator arrayIterator2 = new ArrayIterator(v2, nrsmd);

		System.out.println("AI ***");
		arrayIterator.display();
		arrayIterator2.display();
		Predicate p = new SimplePredicate(0, 2);
		MemoryHashJoin memoryHashJoin = new MemoryHashJoin(arrayIterator,
				arrayIterator2, p);
		memoryHashJoin.open();
		System.out.println("WHILE");
		while (memoryHashJoin.hasNext()) {
			NTuple nt = memoryHashJoin.next();
			int[] is = nt.integerFields;
			nt.display();
		}
	}

}
