package com.github.mistertea.zombiedb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junitx.framework.ComparableAssert;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.github.mistertea.zombiedb.engine.DatabaseEngine;
import com.github.mistertea.zombiedb.thrift.TestThrift;

@Ignore public class DatabaseTestBase {
	DatabaseEngine db;
	DatabaseEngineManager dbm;
	IndexedDatabaseEngineManager idbm;

	private static final String CHARACTERS = "123456789qwertyuiopasdfghjklzxcvbnm";

	private String generateString(Random random, int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = CHARACTERS.charAt(random.nextInt(CHARACTERS.length()));
	    }
	    return new String(text);
	}

	private TestThrift getLast(ArrayList<? extends TestThrift> arrayList) {
		return arrayList.get(arrayList.size()-1);
	}

	@Test public void startServer() throws Exception {
	}
	
	@Test public void testCreate() throws Exception {
		dbm.wipeDatabase();
		
		Random r = new Random(1L);
		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			dbm.create(tt);
			dbm.commit();
			
			TestThrift ttReturn = dbm.get(tt.getClass(), tt.id);
			
			Assert.assertEquals(tt, ttReturn);
		}

		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(String.valueOf(test), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			dbm.createWithId(tt);
			dbm.commit();
			
			TestThrift ttReturn = dbm.get(tt.getClass(), tt.id);
			
			Assert.assertEquals(tt, ttReturn);
		}
	}
	
	@Test public void testCreateDelete() throws Exception {
		dbm.wipeDatabase();
		
		Random r = new Random(1L);
		List<TestThrift> tts = new ArrayList<TestThrift>();
		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			dbm.create(tt);
			tts.add(tt);
			
			dbm.commit();
		}
		
		for(TestThrift tt : tts) {
			TestThrift ttModified = new TestThrift(tt.id, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			Assert.assertNotNull(dbm.get(TestThrift.class, tt.id));
			dbm.delete(ttModified);
			dbm.commit();
			Assert.assertNull(dbm.get(TestThrift.class, tt.id));
		}
		
		dbm.wipeDatabase();
	}
	
	@Test public void testCreateDeleteIndexed() throws Exception {
		idbm.wipeDatabase();
		
		Random r = new Random(1L);
		List<TestThrift> tts = new ArrayList<TestThrift>();
		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.create(tt);
			tts.add(tt);
			
			idbm.commit();
		}
		
		for(TestThrift tt : tts) {
			int ids = idbm.secondaryGet(tt.getClass(), "id", tt.id).size();
			int bs = idbm.secondaryGet(tt.getClass(), "b", tt.b).size();
			ComparableAssert.assertGreater(0, ids);
			ComparableAssert.assertGreater(0, bs);
			
			TestThrift ttModified = new TestThrift(tt.id, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.delete(ttModified);
			idbm.commit();

			int ids2 = idbm.secondaryGet(tt.getClass(), "id", tt.id).size();
			int bs2 = idbm.secondaryGet(tt.getClass(), "b", tt.b).size();
			
			Assert.assertEquals(ids-1 , ids2);
			Assert.assertEquals(bs-1 , bs2);
		}
		
		idbm.wipeDatabase();
	}
	
	@Test public void testCreateIndexed() throws Exception {
		idbm.wipeDatabase();
		
		Random r = new Random(1L);
		for(int test=0;test<100;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.create(tt);
			idbm.commit();
			
			//System.out.println(tt);
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "id", tt.id)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "i", tt.i)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "l", tt.l)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "b", tt.b)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "by", tt.by)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "s", tt.s)));
			try {
				idbm.secondaryGet(tt.getClass(), "d", tt.d);
				Assert.fail("Expected an exception");
			} catch (IOException e) {
			}
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "st", tt.st)));
			try {
				idbm.secondaryGet(tt.getClass(), "notIndexedString", tt.notIndexedString);
				Assert.fail("Expected an exception");
			} catch (IOException e) {
			}
		}
		
		for(int test=0;test<100;test++) {
			// NOTE(jgauci): We never have collisions with the ids above
			TestThrift tt = new TestThrift(String.valueOf(100000 + r.nextInt(100000)), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.createWithId(tt);
			idbm.commit();
			
			//System.out.println(tt);
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "id", tt.id)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "i", tt.i)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "l", tt.l)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "b", tt.b)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "by", tt.by)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "s", tt.s)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "st", tt.st)));
			try {
				idbm.secondaryGet(tt.getClass(), "d", tt.d);
				Assert.fail("Expected an exception");
			} catch (IOException e) {
			}
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "st", tt.st)));
			try {
				idbm.secondaryGet(tt.getClass(), "notIndexedString", tt.notIndexedString);
				Assert.fail("Expected an exception");
			} catch (IOException e) {
			}
		}

		for(int test=0;test<100;test++) {
			// NOTE(jgauci): We never have collisions with the ids above
			TestThrift tt = new TestThrift(String.valueOf(200000 + r.nextInt(100000)), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), null, "abc");
			idbm.createWithId(tt);
			idbm.commit();
			
			//System.out.println(tt);
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "id", tt.id)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "i", tt.i)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "l", tt.l)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "b", tt.b)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "by", tt.by)));
			Assert.assertEquals(tt, getLast(idbm.secondaryGet(tt.getClass(), "s", tt.s)));
			try {
				idbm.secondaryGet(tt.getClass(), "d", tt.d);
				Assert.fail("Expected an exception");
			} catch (IOException e) {
			}
			Assert.assertEquals(0, idbm.secondaryGet(tt.getClass(), "st", tt.st).size());
			try {
				idbm.secondaryGet(tt.getClass(), "notIndexedString", tt.notIndexedString);
				Assert.fail("Expected an exception");
			} catch (IOException e) {
			}
		}
		
		idbm.wipeDatabase();
	}
	
	@Test public void testCreateIndexedSpeed() throws Exception {
		idbm.wipeDatabase();
		
		Random r = new Random(1L);
		for(int test=0;test<1000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.create(tt);
			
			idbm.commit();
		}
		
		for(int test=0;test<1000;test++) {
			// NOTE(jgauci): We never have collisions with the ids above
			TestThrift tt = new TestThrift(String.valueOf(100000 + r.nextInt()), r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			idbm.createWithId(tt);
			
			idbm.commit();
		}

		idbm.wipeDatabase();
	}
	
	@Test public void testRaw() throws Exception {
		db.wipeDatabase();
		
		Random r = new Random(1L);
		for(int test=0;test<100;test++) {
			byte[][] b = new byte[100][16];
			for(int i=0;i<100;i++) {
				r.nextBytes(b[i]);
			}
			
			for(int i=0;i<100;i++) {
				db.putBytes("MyClass", "key"+i, b[i]);
				db.commit();
				byte[] ret = db.getBytes("MyClass", "key"+i);
				Assert.assertArrayEquals(b[i], ret);
			}
		}
	}

	@Test public void testSerialize() throws Exception {
		dbm.wipeDatabase();
		Random r = new Random(1L);
		Set<TestThrift> tts = new HashSet<TestThrift>();
		for(int test=0;test<10000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			tts.add(tt);
			dbm.create(tt);
			dbm.commit();
		}
		
		dbm.dump(TestThrift.class, ".");
		dbm.wipeDatabase();

		dbm.load(TestThrift.class, ".");
		for(TestThrift tt : tts) {
			TestThrift tt2 = dbm.get(TestThrift.class, tt.id);
			Assert.assertEquals(tt, tt2);
		}

		new File("TestThrift.sf").delete();
	}
	@Test public void testSerializeIndexed() throws Exception {
		idbm.wipeDatabase();
		Random r = new Random(1L);
		Set<TestThrift> tts = new HashSet<TestThrift>();
		for(int test=0;test<10000;test++) {
			TestThrift tt = new TestThrift(null, r.nextInt(), r.nextLong(), r.nextBoolean(), (byte)r.nextInt(),
					(short)r.nextInt(), r.nextDouble(), generateString(r, 16), "abc");
			tts.add(tt);
			idbm.create(tt);
			idbm.commit();
		}
		
		idbm.dump(TestThrift.class, ".");
		idbm.wipeDatabase();

		idbm.load(TestThrift.class, ".");
		for(TestThrift tt : tts) {
			TestThrift tt2 = idbm.get(TestThrift.class, tt.id);
			Assert.assertEquals(tt, tt2);
		}

		new File("TestThrift.sf").delete();
		new File(".TestThrift.sf.crc").delete();
	}
}
