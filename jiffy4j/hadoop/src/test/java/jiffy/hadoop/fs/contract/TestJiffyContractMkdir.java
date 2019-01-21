package jiffy.hadoop.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestJiffyContractMkdir extends AbstractContractMkdirTest {

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
