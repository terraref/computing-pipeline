function nargout = PlotFluorescenceFeatures(Fm_dark,Fv_dark,FvFm_dark,Fm_light,Fv_light,FvFm_light,Phi_PSII,NPQ,qN,qP,Rfd)
  
 % Variables used here
% Fm_dark        - 1936-by-1216          double - Fm Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv_dark        - 1936-by-1216          double - Fv Fm_dark - F0_dark 

% Fm_light       - 1936-by-1216         double - Fm' Max. fluorescence level for dark-adapted plants following the staturation pulse typically after 0.5s reached
% Fv_light       - 1936-by-1216         double - Fv' Fm_dark - F0_dark 
% Ft_light       - 1936-by-1216         double - Ft  steady-state flourescence in the light 

% computed values
% FvFm_dark      -  1936-by-1216        double  Fv_dark/Fm_dark The maximal photochemical effiency of PSII
% FvFm_light     -  1936-by-1216        double  Fv_dark/Fm_dark The maximal photochemical effiency of PSII
% Phi_PSII       -  1936-by-1216        double  Quantum yield of photosynthesis
% NPQ            -  1936-by-1216        double  Non-photochemical quenching, absorbed light energy that is dissipated (mostly by thermal radiation)
% qN             -  1936-by-1216        double  Proportion of closed PSII reaction centers
% qP             -  1936-by-1216        double  Proportion of open PSII reaction centers
% Rfd            -  1936-by-1216        double  ratio of chlorophyll decrease to steady state Chlorophyll 
  

 SavePath='C:\7100019-USFieldSystem\Matlab\PS2\20170523-PS2\';
  

  
 colormap('hot') 
 figure(1), clf, title('Fm dark adapted plants')
 FmHist=reshape(Fm_dark,1,1936*1216);
figure(1),subplot(1,2,1), hist(FmHist(FmHist>0),20), xlim([0,1]),xlabel("Fm"),ylabel("no of pixel"), title('Fm dark adapted plants')
set(gca,'Position',     position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(1),subplot(1,2,2),imagesc(Fm_dark),colorbar, axis off
saveas (gcf, [SavePath "Fig_FmDark.png"]);

 colormap('hot') 
 figure(2), clf, title('Fv dark adapted plants')
 Fv_dark(Fv_dark<0)=0;
 FvHist=reshape(Fv_dark,1,1936*1216);
figure(2),subplot(1,2,1), hist(FvHist(FvHist>0),20), xlim([0,1]),xlabel("Fv"),ylabel("no of pixel"), title('Fv dark adapted plants')
set(gca,'Position',     position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(2),subplot(1,2,2),imagesc(Fv_dark),colorbar, axis off
saveas (gcf, [SavePath "Fig_FvDark.png"]);

 colormap('hot') 
 figure(3), clf, title('Fv/Fm dark adapted plants')
 FvFmHist=reshape(FvFm_dark,1,1936*1216);
figure(3),subplot(1,2,1), hist(FvFmHist(FvFmHist>0),20), xlim([0,1]),xlabel("Fv/Fm"),ylabel("no of pixel"), title('Fv /Fm dark adapted plants')
set(gca,'Position',     position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(3),subplot(1,2,2),imagesc(FvFm_dark),colorbar, axis off
saveas (gcf, [SavePath "Fig_FvFmDark.png"]);
 
 colormap('hot') 
figure(4), clf, title('Fm light adapted plants')
FmHist=reshape(Fm_light,1,1936*1216);
figure(4),subplot(1,2,1), hist(FmHist(FmHist>0.01),20), xlim([0,1]),xlabel("Fm"),ylabel("no of pixel"), title('Fm light adapted plants')
set(gca,'Position',     position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(4),subplot(1,2,2),imagesc(Fm_light),colorbar, axis off
saveas (gcf, [SavePath "Fig_FmLight.png"]);

 colormap('hot') 
figure(5), clf, title('Fm light adapted plants')
FvHist=reshape(Fv_light,1,1936*1216);
figure(5),subplot(1,2,1), hist(FvHist(FvHist>0.01),20), xlim([0,1]),xlabel("Fv"),ylabel("no of pixel"), title('Fv light adapted plants')
set(gca,'Position',     position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(5),subplot(1,2,2),imagesc(Fv_light),colorbar, axis off  
saveas (gcf, [SavePath "Fig_FvLight.png"]);  

 colormap('hot') 
 figure(6), clf, title('Fv/Fm light adapted plants')
 FvFmHist=reshape(FvFm_light,1,1936*1216);
figure(6),subplot(1,2,1), hist(FvFmHist(FvFmHist>0),20), xlim([0,1]),xlabel("Fv/Fm"),ylabel("no of pixel"), title('Fv /Fm light adapted plants')
set(gca,'Position',     position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(6),subplot(1,2,2),imagesc(FvFm_light),colorbar, axis off
saveas (gcf, [SavePath "Fig_FvFmLight.png"]); 

 colormap('hot') 
figure(7), clf, title('Phi PSII')
Phi_PSIIHist=reshape(Phi_PSII,1,1936*1216);
figure(7),subplot(1,2,1), hist(Phi_PSII),20,xlabel("Phi PSII"),ylabel("no of pixel"), title('Phi_PSII')
set(gca,'Position', position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
figure(7),subplot(1,2,2),imagesc(Phi_PSII),colorbar, axis off
saveas (gcf, [SavePath "Fig_Phi_PS2.png"]); 

colormap('hot') 
figure(8), clf, title('NPQ')
NPQHist=reshape(NPQ,1,1936*1216);
figure(8),subplot(1,2,1), hist(NPQ(~isnan(NPQ)),20),xlabel("NPQ"),ylabel("no of pixel"), title('NPQ')
set(gca,'Position', position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
NPQ(isnan(NPQ))=0;
figure(8),subplot(1,2,2),imagesc(NPQ),colorbar, axis off
saveas (gcf, [SavePath "NPQ.png"]); 

colormap('hot') 
figure(9), clf, title('qN')
qNHist=reshape(qN,1,1936*1216);
figure(9),subplot(1,2,1), hist(qNHist(~isinf(qNHist)),20),xlabel("qN"),ylabel("no of pixel"), title('gN')
set(gca,'Position', position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
qN(isnan(qN))=0;
figure(9),subplot(1,2,2),imagesc(qN),colorbar, axis off
saveas (gcf, [SavePath "qN.png"]); 

colormap('hot') 
figure(10), clf, title('qP')
qPHist=reshape(qP,1,1936*1216);
figure(10),subplot(1,2,1), hist(qPHist(~isinf(qPHist)),20),xlabel("qP"),ylabel("no of pixel"), title('gP')
set(gca,'Position', position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
qP(isnan(qP))=0;
figure(10),subplot(1,2,2),imagesc(qP),colorbar, axis off
saveas (gcf, [SavePath "qP.png"]); 

colormap('hot') 
figure(11), clf, title('Rfd')
RfdHist=reshape(Rfd,1,1936*1216);
figure(11),subplot(1,2,1), hist(RfdHist(~isnan(RfdHist)),20),xlabel("RfD"),ylabel("no of pixel"), title('RfD')
set(gca,'Position', position = [0.13000   0.11000+0.2   0.31127   0.81500-0.4])
Rfd(isnan(Rfd))=0;
figure(11),subplot(1,2,2),imagesc(Rfd),colorbar, axis off
saveas (gcf, [SavePath "Rfd.png"]); 



  
