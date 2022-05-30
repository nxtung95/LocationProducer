/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package producer;

import java.util.Set;
import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.core.Application;

/**
 *
 * @author aensor
 */
@ApplicationPath("webresources")
public class ApplicationConfig extends Application
{

   @Override
   public Set<Class<?>> getClasses()
   {
      Set<Class<?>> resources = new java.util.HashSet<>();
      addRestResourceClasses(resources);
      return resources;
   }

   /**
    * Do not modify addRestResourceClasses() method.
    * It is automatically populated with
    * all resources defined in the project.
    * If required, comment out calling this method in getClasses().
    */
   private void addRestResourceClasses(Set<Class<?>> resources)
   {
   }
   
}
