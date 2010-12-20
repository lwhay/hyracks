package edu.uci.ics.hyracks.api.resources;

public interface IResourceDeallocator {
    public void addDeallocatableResource(IDeallocatable resource);
}