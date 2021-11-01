package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SourceBean implements PmuIdBean {
    public int PMU_ID;
    public long Timestamp;

    public double Mag_VA1;//Mag VA1 [V]
    public double Phase_VA1;// Phase VA1 [rad]

    public double Mag_VB1;// Mag VB1 [V]
    public double Phase_VB1;// Phase VB1 [rad]

    public double Mag_VC1;// Mag VC1 [V]
    public double Phase_VC1;// Phase VC1 [rad]

    public double Mag_IA1;// Mag IA1 [A]
    public double Phase_IA1;// Phase IA1 [rad]

    public double Mag_IB1;// Mag IB1 [A]
    public double Phase_IB1;// Phase IB1 [rad]

    public double Mag_IC1;// Mag IC1 [A]
    public double Phase_IC1;// Phase IC1 [rad]

    @Override
    public int getPmuId() {
        return this.PMU_ID;
    }
}
