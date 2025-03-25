/*
 * Decompiled with CFR 0.152.
 */
package SparkER.DataStructures;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class EntityProfile
implements Serializable {
    private static final long serialVersionUID = 122354534453243447L;
    private final Set<Attribute> attributes;
    private final String entityUrl;

    public EntityProfile(String url) {
        this.entityUrl = url;
        this.attributes = new HashSet<Attribute>();
    }

    public void addAttribute(String propertyName, String propertyValue) {
        this.attributes.add(new Attribute(propertyName, propertyValue));
    }

    public String getEntityUrl() {
        return this.entityUrl;
    }

    public int getProfileSize() {
        return this.attributes.size();
    }

    public Set<Attribute> getAttributes() {
        return this.attributes;
    }
}
