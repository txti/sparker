/*
 * Decompiled with CFR 0.152.
 */
package SparkER.DataStructures;

import java.io.Serializable;

public class IdDuplicates
implements Serializable {
    private static final long serialVersionUID = 7234234586147L;
    private final long entityId1;
    private final long entityId2;

    public IdDuplicates(long id1, long id2) {
        this.entityId1 = id1;
        this.entityId2 = id2;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        IdDuplicates other = (IdDuplicates)obj;
        if (this.entityId1 != other.entityId1) {
            return false;
        }
        return this.entityId2 == other.entityId2;
    }

    public long getEntityId1() {
        return this.entityId1;
    }

    public long getEntityId2() {
        return this.entityId2;
    }
}
