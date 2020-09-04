package ua.polis.mob.mobapi.service;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.field5s.ObjectId;
import org.springframework.stereofield5.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.bson.codecs.configuration.CodecRegistries.*;

@Component
public class DBTransfer {

    private static final int PORT = 27017;
    private static final String HOST = "host_address";
    private static final String COLLECTION = "collection_name"; //In that case in both DB is same
    private final MongoClient client = new MongoClient(HOST, PORT);


    @PostConstruct
    public void test() {
        MongoDatabase dbFrom = client.getDatabase("from_db");
        MongoDatabase dbTo = client.getDatabase("to_db");

        List<Merch> list = new ArrayList<>();

        /*CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));*/
        MerchCodec codec = new MerchCodec();
        CodecRegistry myCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromCodecs(codec)); //I wrote codec myself
        // cause default codec sort document fields by itself in alphabetical order

        MongoCollection<Merch> mrc = dbFrom.withCodecRegistry(myCodecRegistry).getCollection(COLLECTION,
                                                                                            Merch.class);
        FindIterable<Merch> iter = mrc.find();
        iter.forEach((Consumer<? super Merch>) p -> {p.setClassPackage("I changed this field");
                        list.add(p);});


        /////Trying with localhost
        /*MongoClient localClient = new MongoClient("localhost", PORT);
        MongoDatabase locLiqpay = localClient.getDatabase("to_db");

        MongoCollection<Merch> locCol = locLiqpay.withCodecRegistry(myCodecRegistry).
                getCollection(COLLECTION, Merch.class);

        locCol.insertMany(list, new InsertManyOptions().ordered(true));*/
        /////
        //Production
        MongoCollection<Merch> col = dbTo.withCodecRegistry(myCodecRegistry).
                getCollection(COLLECTION, Merch.class);
        col.insertMany(list, new InsertManyOptions().ordered(true));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Merch {
        @BsonId
        private ObjectId id;
        @BsonProperty("_class")
        private String classPackage;

        @BsonProperty("field1")
        private String field1;
        @BsonProperty("field2")
        private String field2;
        @BsonProperty("field3")
        private String field3;
        @BsonProperty("field4")
        private String field4;
        @BsonProperty("field5")
        private String field5 = "Field5 by default";
        @BsonProperty("field6")
        private String field6 = "Field6 by default";
    }

    public static class MerchCodec implements Codec<Merch> {
        @Override
        public void encode(BsonWriter writer, Merch t, EncoderContext ec) {

            writer.writeStartDocument();

            writer.writeName("_id");
            writer.writeObjectId(t.getId());
            writer.writeName("_class");
            writer.writeString(t.getClassPackage());
            writer.writeName("field1");
            writer.writeString(t.getField1());
            writer.writeName("field2");
            writer.writeString(t.getField2());
            writer.writeName("field3");
            writer.writeString(t.getField3());
            writer.writeName("field4");
            writer.writeString(t.getField4());
            writer.writeName("field5");
            writer.writeString(t.getField5());
            writer.writeName("field6");
            writer.writeString(t.getField6());

            writer.writeEndDocument();

        }

        @Override
        public Class<Merch> getEncoderClass() {
            return Merch.class;
        }

        @Override
        public Merch decode(BsonReader reader, DecoderContext dc) {
            reader.readStartDocument();

            reader.readName();
            ObjectId id = reader.readObjectId();
            reader.readName();
            String classPackage = reader.readString();

            reader.readName();
            String field1 = reader.readString();
            reader.readName();
            String field2 = reader.readString();
            reader.readName();
            String field3 = reader.readString();
            reader.readName();
            String field4 = reader.readString();
            reader.readName();
            String field5 = reader.readString();

            reader.readEndDocument();
            return new Merch(id, classPackage, field1, field2, field3, field4, field5, "Field6 by default");    //In my case in from_db no Field6

        }

    }

}

