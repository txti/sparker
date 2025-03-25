/*
 * Decompiled with CFR 0.152.
 */
package SparkER.DataStructures;

import java.io.Serializable;

public class Attribute
implements Serializable {
    private static final long serialVersionUID = 1245324342344634589L;
    private final String name;
    private final String value;

    public Attribute(String nm, String val) {
        this.name = nm;
        this.value = val;
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        Attribute other = (Attribute)obj;
        if (this.name == null ? other.name != null : !this.name.equals(other.name)) {
            return false;
        }
        return !(this.value == null ? other.value != null : !this.value.equals(other.value));
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }
}
