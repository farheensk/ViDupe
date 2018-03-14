package vidupe.ffmpeg.textualFiles;

import java.util.List;

import com.google.api.services.drive.model.File;

public class GoogleDetails
{
  String id;
  String email;
  boolean verified_email;
  String name;
  String given_name;
  String family_name;
  int length;
  List<File> files;
FilteredVideos[] filteredVideoFiles;
public void setLenght(int length) {
	this.length=length;
}
public int getLength() {
	return this.length;
}

  public void setFilteredVideoFiles(FilteredVideos filteredVideoFiles[]) {
	   
	  this.filteredVideoFiles=new FilteredVideos[length];
	  	 for(int i=0;i<length;i++) {
	  		 this.filteredVideoFiles[i]=new FilteredVideos();
	  		 this.filteredVideoFiles[i].setVideoID(filteredVideoFiles[i].getVideoID());
	  		 this.filteredVideoFiles[i].setVideoName(filteredVideoFiles[i].getVideoName());
	  		 this.filteredVideoFiles[i].setDescription(filteredVideoFiles[i].getDescription());
	 	  	 }
		
	 }
  
  public FilteredVideos[] getFilteredVideoFiles() {
	      for(int i=0;i<length;i++) {
	    	  this.filteredVideoFiles[i].getVideoID();
	    	  this.filteredVideoFiles[i].getVideoName();
	    	  this.filteredVideoFiles[i].getDescription();
	    	  
	      }
		  return this.filteredVideoFiles;
	  } 
  
 public void setFiles(List<File>files) {
	 this.files=files;
 }
  public List<File> getFiles() {
	  return this.files;
  }
  
  
  public String getId()
  {
    return this.id;
  }
  
  public void setId(String id)
  {
    this.id = id;
  }
  
  public String getEmail()
  {
    return this.email;
  }
  
  public void setEmail(String email)
  {
    this.email = email;
  }
  
  public boolean isVerified_email()
  {
    return this.verified_email;
  }
  
  public void setVerified_email(boolean verified_email)
  {
    this.verified_email = verified_email;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public String getGiven_name()
  {
    return this.given_name;
  }
  
  public void setGiven_name(String given_name)
  {
    this.given_name = given_name;
  }
  
  public String getFamily_name()
  {
    return this.family_name;
  }
  
  public void setFamily_name(String family_name)
  {
    this.family_name = family_name;
  }
  
  public String toString()
  {
    return 
    
      "GooglePojo [id=" + this.id + ", email=" + this.email + ", verified_email=" + this.verified_email + ", name=" + this.name + ", given_name=" + this.given_name + ", family_name=" + this.family_name + "]";
  }
}
