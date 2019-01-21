package jiffy.hadoop.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestJiffyContractDelete extends AbstractContractDeleteTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new JiffyContract(conf);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    ((JiffyContract) getContract()).close();
  }
}
