package jtest;

import com.oracle.labs.mlrg.olcut.config.Configurable;
import com.oracle.labs.mlrg.olcut.config.ConfigurationData;
import com.oracle.labs.mlrg.olcut.config.ConfigurationManager;
import com.oracle.labs.mlrg.olcut.config.DescribeConfigurable;
import com.oracle.labs.mlrg.olcut.config.property.Property;
import com.oracle.labs.mlrg.olcut.config.property.SimpleProperty;
import org.tribuo.classification.dtree.CARTClassificationTrainer;
import org.tribuo.classification.dtree.impurity.Entropy;
import org.tribuo.classification.dtree.impurity.GiniIndex;
import org.tribuo.classification.dtree.impurity.LabelImpurity;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

public class ConfigTest {

    public static final  void describe () {

    }
    public static void main(String[] args) {

        Class clazz = CARTClassificationTrainer.class;

        SortedMap<String, DescribeConfigurable.FieldInfo> generateFieldInfos = DescribeConfigurable.generateFieldInfo(clazz);


        //System.out.println("DescribeConfigurable.generateFieldInfo(CARTClassificationTrainer.class) = " + generateFieldInfo);
        ConfigurationManager cm = new ConfigurationManager();
        DescribeConfigurable dc = new DescribeConfigurable();



        Map<String, Property> properties = Map.of(
                "maxDepth", new SimpleProperty("20"),
                "minImpurityDecrease",new SimpleProperty("0.5"),
                "impurity",new SimpleProperty("entropy")
        );
        ConfigurationData configData0 = new ConfigurationData("entropy", Entropy.class.getName());
        cm.addConfiguration(configData0);
        ConfigurationData configData1 = new ConfigurationData(CARTClassificationTrainer.class.getName(), clazz.getName(),properties);

        cm.addConfiguration(configData1);
        Configurable lookup = cm.lookup(clazz.getName());
        System.out.println(lookup);


    }
}
