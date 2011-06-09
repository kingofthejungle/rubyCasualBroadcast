# ***** http://www.info.ucl.ac.be/courses/LINF2345/Lectures/Lecture3-causallBroadcast.pdf
# ***   http://pi1.informatik.uni-mannheim.de/filepool/teaching/dependablesystems-2006/PDS_20060517.pdf

THREAD_CNT = 3


$processes = []


# stopnuti threadu na interrupt
trap("SIGINT") do
    puts "\nSIGINT - terminating threads"
    0.upto(THREAD_CNT-1) do |i|
        $processes[i].terminate!
    end
end

#
# trida reprezentujici jeden proces
class Worker

  def initialize(id)
    @pid = id
    @vc = Array.new
    @pending = Array.new

    # naplneni pocatecnimi hodnotami
    THREAD_CNT.downto(1) { |i|
      @vc[i-1] = 0
    }
    puts "inicializace procesu "+@pid.to_s
  end

  def pid
    @pid
  end

  def rb_broadcast(m)
    puts @pid.to_s+" rb_broadcast: "+m
    $processes.each { |process|

      # nutno naklonovat promennou, aby zustala a neprepsala se dalsim broadcastem
      vc = @vc.clone
      #puts @pid.to_s+" vc: "+vc.to_s
      Thread.new(vc) { |vc|
        # uspani simulujici zpozdeni v doruceni
        rand = rand(3)+1
        #puts @pid.to_s+"spi pri "+m+" - "+rand.to_s
        sleep(rand)

        process[:object].rb_deliver(@pid, {:type => "DATA", :vc => vc, :m => m})
      }
    }
  end


  # reliable casual order broadcast
  def rco_broadcast(m)
    puts @pid.to_s+" rco_broadcast: "+m
    self.rb_broadcast(m)
    self.rco_deliver(@pid, m)
  end


  # doruceni zpravy
  # TODO: dopsat az se dozvim co to dela
  def rco_deliver(pj, m)
    puts @pid.to_s+" rco_deliver "+m+" od "+pj.to_s
    @vc[pj] += 1
  end

  
  # pj je pid odesilajiciho procesu 
  # data je zde (DATA, VCm, m)
  def rb_deliver(pj, data)
    if pj != @pid
      puts @pid.to_s+" rb_deliver "+data[:m].to_s+" od "+pj.to_s+" s vc:"+data[:vc].to_s

      @pending << [pj, data]
      # potreba nekolikrat zavolat deliver_pending kvuli podmince while exists
      # prvni zprava v pending se totiz muze preskocit, pak se doruci druha
      # a az pak je potreba dorucit tu prvni... tohle je oklika pred slozitym cyklem
      # a podminkami
      1.upto(THREAD_CNT) {
        self.deliver_pending
      }
    end
  end


  # doruci cekajici zpravy
  def deliver_pending
    delete_at = Array.new    
    @pending.each_with_index do |pending_item, i|
      #puts @pid.to_s+" porovnani "+pending_item[1][:m]+" index: "+pending_item[0].to_s+" v "+@vc.to_s+" a "+pending_item[1][:vc].to_s

      if @vc[pending_item[0]] >= pending_item[1][:vc][pending_item[0]]
        #puts "je vetsi"
        @vc[pending_item[0]] += 1
        $processes[pending_item[0]][:object].rco_deliver(@pid, pending_item[1][:m])
        delete_at << i
      end
    end
    #smazani pending
    delete_at.each {|i|
      @pending.delete_at(i)
    }
  end

  # hlavni metoda pro beh vlakna
  def run
    while 1
      sleep(0.2)
      #puts "bezim "+@pid.to_s
    end
  end
end


#spusteni procesu
0.upto(THREAD_CNT-1) do |i|
    $processes[i] = Thread.new(i) do
      ct = Thread.current
      ct[:object] = Worker.new(i)
      ct[:object].run
    end
end


#sleep(1)


$processes[0][:object].rco_broadcast("m1")
$processes[0][:object].rco_broadcast("m2")


# cekani na vsechny thready v hlavnim threadu
# program se neukonci
0.upto(THREAD_CNT-1) do |i|
  $processes[i].join
end


