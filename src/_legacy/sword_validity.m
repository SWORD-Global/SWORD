% Options
global opt_create_csv opt_create_json opt_create_txt
opt_reverse_3b=1; % Allows to reverse a connected Reach when pb 3b (0: no, 1: yes if n_rch_up_rch/n_rch_dn_rch is good, 2: always)
opt_reverse_4b=1; % Allows to reverse a connected Reach when pb 4b (0: no, 1: yes if n_rch_up_rch/n_rch_dn_rch is good, 2: always)
opt_reverse_11=2; % Allows to reverse Nodes inside a Reach (0: no, 1: yes when improves or same nb of good up & dn connections with geoloc, 2: when strictly improves)
opt_warning_3e=0; % Check if some suspicious (but possible) cases may appear (0: no, 1: yes)
opt_warning_4e=0; % Check if some suspicious (but possible) cases may appear (0: no, 1: yes)
opt_warning_6b=2; % Check the geoloc Node dist problem only for main_side_rch <= opt_warning_6b (0, 1 or 2)
opt_warning_7=0; % Check the dist_out problem only for main_side_rch <= opt_warning_7 (0, 1 or 2)
opt_warning_10ab=0; % Check the dist_out problem only for main_side_nod <= opt_warning_10ab (0, 1 or 2)
opt_warning_10cd=2; % Check the dist_out problem only for main_side_nod <= opt_warning_10cd (0, 1 or 2)
opt_warning_12a=1; % Check if Nodes are ordered in memory from downstream to upstream (0: no, 1: yes). Not a bug in fact (cf Elizabeth Altenau 2 April 2025)
nb_case_test=0;
list_case_test=[];
% Initialisation
% rch_sos_inversed=[]; % Maybe to do here? To start from nil when validity is check on the subset!? (POM 20/12/2023)
% Management of the csv and json file in case of automatic correction
if opt_sword_corr_auto>0
    mode_correction='automatic';
    if opt_create_csv>0
        % Vérification ou création sous répertoire csv
        filename='Sword\csv\';
        if ~exist(filename,"dir")
            mkdir(filename);
            disp(['Creation of directory ' filename])
        end
    end
    if opt_create_json>0
        % Vérification ou création sous répertoire json
        filename='Sword\json\';
        if ~exist(filename,"dir")
            mkdir(filename);
            disp(['Creation of directory ' filename])
        end
    end
    if opt_create_txt>0
        % Vérification ou création sous répertoire txt
        filename='Sword\txt\';
        if ~exist(filename,"dir")
            mkdir(filename);
            disp(['Creation of directory ' filename])
        end
    end
    if opt_create_json>0
        % Création du fichier json
        if opt_filter_validity==0
            filename=['Sword\json\sword_patches_' mode_correction '_' cas_sword_region '_' cas_sword_version '_' current_time '.json'];
        else
            filename=['Sword\json\sword_patches_' mode_correction '_' nom_riv '_' cas_sword_version '_' current_time '.json'];
        end
        fidJson=fopen(filename,'w');
        struct_patch_json=[]; % On vide la structure pour le json file
        struct_reach=[]; % On vide la structure pour les Reaches dont la connectivité est à modifier (plusieurs tests peuvent y contribuer, donc on doit lister les modifs avant de modifier après Tests 5)
    end
    if opt_create_csv>0
        % Nettoyage des fichiers csv
        if opt_sword_clean_csv
            for Report_Index=1:6
                % Ancien sous-repertoire des fichiers csv
                filename=['Correct_Sword_' nom_riv '_' num2str(Report_Index) '.csv'];
                if exist(filename,"file")
                    delete(filename);
                    disp(['Delete file ' filename])
                end
                if length(list_sub_river)>1
                    filename=['Sword\csv\Correct_Sword_' mode_correction '_br' num2str(base_river) 'ri' num2str(river) 'sr' num2str(list_sub_river(1)) '-' num2str(list_sub_river(end)) '_' num2str(Report_Index) '_' cas_sword_version '.csv'];
                else
                    filename=['Sword\csv\Correct_Sword_' mode_correction '_' nom_riv '_' num2str(Report_Index) '_' cas_sword_version '.csv'];
                end
                if (length(list_sub_river)>1 && sub_river==list_sub_river(1)) || length(list_sub_river)==1
                    if exist(filename,"file")
                        delete(filename);
                        disp(['Delete file ' filename])
                    end
                end
            end
        end
    end
    if opt_create_txt>0
        % Nettoyage des fichiers txt
        if opt_sword_clean_txt
            if length(list_sub_river)>1
                filename=['Sword\txt\Correct_Sword_' mode_correction '_br' num2str(base_river) 'ri' num2str(river) 'sr' num2str(list_sub_river(1)) '-' num2str(list_sub_river(end)) '_' cas_sword_version '.txt'];
            else
                filename=['Sword\txt\Correct_Sword_' mode_correction '_' nom_riv '_' cas_sword_version '.txt'];
            end
            if (length(list_sub_river)>1 && sub_river==list_sub_river(1)) || length(list_sub_river)==1
                if exist(filename,"file")
                    delete(filename);
                    disp(['Delete file ' filename])
                end
            end
        end
    end
    disp(['Automatic correction of the Sword database activated!'])
else
    disp(['Automatic correction of the Sword database is NOT activated!'])
end
% =========================================================================
msg_txt=['Production date of Sword file : ' production_date];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Region of Sword file : ' cas_sword_region];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Option allowing to make automatic corrections = ' num2str(opt_sword_corr_auto)];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
if opt_sword_corr_auto>0
    msg_txt=['Number of iterations for corrections = ' num2str(i_run_max)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    msg_txt=['Start correcting at iteration n° = ' num2str(i_run_min)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    msg_txt=['Generation of kml files with Tests results = ' num2str(option_kml_tests)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    msg_txt=['Option allowing to reverse a connected Reach when pb 3b = ' num2str(opt_reverse_3b)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    msg_txt=['Option allowing to reverse a connected Reach when pb 4b = ' num2str(opt_reverse_4b)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    msg_txt=['Option allowing to reverse Nodes inside a Reach = ' num2str(opt_reverse_11)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
end
msg_txt=['Option allowing warnings for tests 3e (eg: no upstream Reach but several downstream) = ' num2str(opt_warning_3e) ' (suspicious (but possible) cases)'];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Option allowing warnings for tests 4e (eg: no downstream Reach but several upstream) = ' num2str(opt_warning_4e) ' (suspicious (but possible) cases)'];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Option allowing warnings for tests 6b (geoloc dist for Nodes) = ' num2str(opt_warning_6b)];
if opt_warning_6b==0
    msg_txt=[msg_txt ' (for main network only)'];
elseif opt_warning_6b==1
    msg_txt=[msg_txt ' (for main and side network only)'];
elseif opt_warning_6b==2
    msg_txt=[msg_txt ' (for main, side network and secondary outlet)'];
end
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Option allowing warnings for tests 7 (dist_out for Reaches) = ' num2str(opt_warning_7)];
if opt_warning_7==0
    msg_txt=[msg_txt ' (for main network only)'];
elseif opt_warning_7==1
    msg_txt=[msg_txt ' (for main and side network only)'];
elseif opt_warning_7==2
    msg_txt=[msg_txt ' (for main, side network and secondary outlet)'];
end
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Option allowing warnings for tests 10ab (dist_out for Nodes inside Reaches) = ' num2str(opt_warning_10ab)];
if opt_warning_10ab==0
    msg_txt=[msg_txt ' (for main network only)'];
elseif opt_warning_10ab==1
    msg_txt=[msg_txt ' (for main and side network only)'];
elseif opt_warning_10ab==2
    msg_txt=[msg_txt ' (for main, side network and secondary outlet)'];
end
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['Option allowing warnings for tests 10cd (dist_out for Nodes between Reaches) = ' num2str(opt_warning_10cd)];
if opt_warning_10cd==0
    msg_txt=[msg_txt ' (for main network only)'];
elseif opt_warning_10cd==1
    msg_txt=[msg_txt ' (for main and side network only)'];
elseif opt_warning_10cd==2
    msg_txt=[msg_txt ' (for main, side network and secondary outlet)'];
end
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
% =========================================================================
% Verify some validity in the sword database structures
lapstime=tic;
msg_txt=['Verification of some properties of the Sword database started...'];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
nb_rch_verified=0;
nb_nod_verified=0;
if opt_filter_validity>0
    msg_txt=['This time just on the ' num2str(size(ref_rch,2)) ' studied Reaches ...'];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    for ii=1:size(ref_rch,2)
        ref_index_rch=ref_rch(ii);
        if id_rch_filter(ref_index_rch)==1
            % We'll verify this Reach
            nb_rch_verified=nb_rch_verified+1;
            nb_nod_verified=nb_nod_verified+n_rch_nod(ref_index_rch);
        end
    end
else
    msg_txt=['On all the ' cas_sword_region ' sword database, having ' num2str(nb_rch) ' Reaches ...'];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    if opt_sword_valid_limit_pfaf_c>0
        for ii=1:nb_rch
            if id_rch_filter(ii)==1
                % We'll verify this Reach
                nb_rch_verified=nb_rch_verified+1;
                nb_nod_verified=nb_nod_verified+n_rch_nod(ii);
            end
        end
    else
        nb_rch_verified=nb_rch;
        nb_nod_verified=nb_nod;
    end
end
if opt_sword_valid_limit_pfaf_c>0
    if size(opt_sword_valid_limit_pfaf_b,2)>1
        msg_txt=['We filter keeping just Reaches with ' num2str(size(opt_sword_valid_limit_pfaf_b,2)) ' Pfafsterrer bassin codes, from ' num2str(opt_sword_valid_limit_pfaf_c) num2str(min(opt_sword_valid_limit_pfaf_b)) ' to ' num2str(opt_sword_valid_limit_pfaf_c) num2str(max(opt_sword_valid_limit_pfaf_b))];
        disp(msg_txt)
    else
        msg_txt=['We filter keeping just Reaches with Pfafsterrer bassin codes ' num2str(opt_sword_valid_limit_pfaf_c) num2str(opt_sword_valid_limit_pfaf_b)];
        disp(msg_txt)
    end
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
end
msg_txt=['We treat finally ' num2str(nb_rch_verified) ' Reaches'];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
msg_txt=['We treat finally ' num2str(nb_nod_verified) ' Nodes'];
disp(msg_txt)
if opt_wrtlog>1
    fprintf(fidLog,'%s\r\n',msg_txt);
end
run_stop=0;
ok_correct_sword_other=0; % We do not save this type of modification, unless for the last run
for i_run=1:i_run_max
    msg_txt=['Verification run n°' num2str(i_run) ' out of max of ' num2str(i_run_max)];
    disp(msg_txt)
    if opt_wrtlog>1
        fprintf(fidLog,'%s\r\n',msg_txt);
    end
    % On remet les infos des tests à 0 (mais c'est un choix, si on veut garder tout l'historique, quitte à faire les kml juste pour le dernier run)
    % Sauf pour les Tests 15.3, car ils ne sont calculés qu'une seule fois (pour i_run=i_run_min)
    % L'idée est qu'en mode sans correction on ait la liste de tout ce qui ne va pas, et qu'en mode correction on ait juste le
    % résultat final en fin des itérations. Dans ce cas on suppose donc que le cas 15.3 n'existe plus et que tout écart sera
    % reporté sur le problème de type 15.4 des Reaches length (sauf si i_run_min=i_run_max et qu'on n'a donc pas traité ce cas)
    if nb_case_test>0
        if opt_sword_corr_auto==0
            data_tests=find(abs(list_case_test(:,2)-15.3)>0.05);
            nb_case_test=size(list_case_test,1)-length(data_tests);
            list_case_test(data_tests,:)=[];
        else
            nb_case_test=0;
            list_case_test=[];
        end
    end
    % ===========================TEST 1 & 2 ===================================
    % Upstream and downstream Reaches
    % Number of connected up & dn Reaches should be correct and coherent
    % between rch_id_up_rch and n_rch_up_rch (and same for dn)
    ok_test1=0; % Test1 type fail count
    ok_test1a=0; % Connected Reaches should be different, ie not duplicated (upstream)
    ok_test1b=0; % Wrong number but no 0 in between (upstream)
    ok_test1c=0; % Wrong number and some 0 in between (upstream)
    ok_test2=0; % Test2 type fail count
    ok_test2a=0; % Connected Reaches should be different, ie not duplicated (downstream)
    ok_test2b=0; % Wrong number but no 0 in between (downstream)
    ok_test2c=0; % Wrong number and some 0 in between (downstream)
    type_test1(1:6)=zeros(1,6);
    type_test2(1:6)=zeros(1,6);
    for i=1:nb_rch
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        % For upstream Reaches (Test 1)
        j_up=find(rch_id_up_rch(i,:)>0); % List of indexes in rch_id_up_rch(i,:) of true Reaches (>0, so in particular not 0)
        r_up_unique=unique(rch_id_up_rch(i,j_up),'stable'); % ID's of unique true Reaches (without the duplicated nor wrong ones, if any)
        n_up=length(r_up_unique); % Number of unique true Reaches (without the duplicated nor wrong ones, if any)
        fl_test1a=0; % Binary flag to tell if a failed Test1a type was activated to proceed for correction
        fl_test1b=0; % Binary flag to tell if a failed Test1b type was activated to proceed for correction
        fl_test1c=0; % Binary flag to tell if a failed Test1c type was activated to proceed for correction
        sub_case1=[]; % Types of failed tests
        if length(j_up)~=n_up % Test if we have duplicates Reaches (removed with the unique command)
            fl_test1a=1;
            ok_test1a=ok_test1a+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=1.1;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=length(j_up)-n_up;
            sub_case1=[sub_case1 "1a"];
            msg_txt=['-> Test 1a failed, some upstream Reaches connected at ' num2str(id_rch(i)) ' (' num2str(i) ') are duplicated'];
            disp(msg_txt)
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
        if n_rch_up_rch(i)~=n_up % Test if the indicated number of Reaches is correct
            fl_test1b=1;
            ok_test1b=ok_test1b+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=1.2;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=n_rch_up_rch(i)~=n_up;
            sub_case1=[sub_case1 "1b"];
            msg_txt=['-> Test 1b failed, wrong number of upstream Reaches connected at ' num2str(id_rch(i)) ' (' num2str(i) '). n=' num2str(n_rch_up_rch(i)) ' (declared) ~= ' num2str(n_up) ' (real)'];
            disp(msg_txt)
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
        if length(j_up)~=max(j_up) % Test if we have ID=0 Reaches in the list rch_id_up_rch(i,j_up)
            fl_test1c=1;
            ok_test1c=ok_test1c+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=1.3;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=length(j_up)-max(j_up);
            sub_case1=[sub_case1 "1c"];
            msg_txt=['-> Test 1c failed, some upstream Reaches indicated as connected at ' num2str(id_rch(i)) ' (' num2str(i) ') have an ID=0'];
            disp(msg_txt)
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
        if (fl_test1a || fl_test1b || fl_test1c)
            ok_test1=ok_test1+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=1.0;
            list_case_test(nb_case_test,3)=i;
            type_test1(id_rch_t(i))=type_test1(id_rch_t(i))+1;
            if opt_sword_corr_auto>0 && i_run>=i_run_min
                % We remove duplicate and 0 Reaches, keep their order, put them first and update the number
                rch_clean(i,-1);
                struct_reach=update_neighbor(struct_reach,i,sub_case1);
            end
        end
        % For downstream Reaches (Test 2)
        j_dn=find(rch_id_dn_rch(i,:)>0); % List of indexes in rch_id_dn_rch(i,:) of true Reaches (>0, so in particular not 0)
        r_dn_unique=unique(rch_id_dn_rch(i,j_dn),'stable'); % ID's of unique true Reaches (without the duplicated nor wrong ones, if any)
        n_dn=length(r_dn_unique); % Number of unique true Reaches (without the duplicated nor wrong ones, if any)
        fl_test2a=0; % Binary flag to tell if a failed Test2a type was activated to proceed for correction
        fl_test2b=0; % Binary flag to tell if a failed Test2b type was activated to proceed for correction
        fl_test2c=0; % Binary flag to tell if a failed Test2c type was activated to proceed for correction
        sub_case2=[];
        if length(j_dn)~=n_dn % Test if we have duplicates Reaches (removed with the unique command)
            fl_test2a=1;
            ok_test2a=ok_test2a+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=2.1;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=length(j_dn)-n_dn;
            sub_case2=[sub_case2 "2a"];
            msg_txt=['-> Test 2a failed, some downstream Reaches connected at ' num2str(id_rch(i)) ' (' num2str(i) ') are duplicated'];
            disp(msg_txt)
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
        if n_rch_dn_rch(i)~=n_dn % Test if the indicated number of Reaches is correct
            fl_test2b=1;
            ok_test2b=ok_test2b+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=2.2;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=n_rch_dn_rch(i)-n_dn;
            sub_case2=[sub_case2 "2b"];
            msg_txt=['-> Test 2b failed, wrong number of downstream Reaches connected at ' num2str(id_rch(i)) ' (' num2str(i) '). n=' num2str(n_rch_dn_rch(i)) ' (declared) ~= ' num2str(n_dn) ' (real)'];
            disp(msg_txt)
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
        if length(j_dn)~=max(j_dn) % Test if we have ID=0 Reaches in the list rch_id_dn_rch(i,j_dn)
            fl_test2c=1;
            ok_test2c=ok_test2c+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=2.3;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=length(j_dn)-max(j_dn);
            sub_case2=[sub_case2 "2c"];
            msg_txt=['-> Test 2c failed, some downstream Reaches indicated as connected at ' num2str(id_rch(i)) ' (' num2str(i) ') have an ID=0'];
            disp(msg_txt)
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
        if (fl_test2a || fl_test2b || fl_test2c)
            ok_test2=ok_test2+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=2.0;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=0;
            type_test2(id_rch_t(i))=type_test2(id_rch_t(i))+1;
            if opt_sword_corr_auto>0 && i_run>=i_run_min
                % We remove duplicate and 0 Reaches, keep their order, put them first and update the number
                rch_clean(i,1);
                struct_reach=update_neighbor(struct_reach,i,sub_case2);
            end
        end
    end
    if ok_test1==0
        msg_txt=['+> Test 1 (a, b and c) passed (number of connected Reaches upstream)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 1 (a:' num2str(ok_test1a) ', b:' num2str(ok_test1b) ' or c:' num2str(ok_test1c) ') failed (wrong number of connected Reaches upstream) ' num2str(ok_test1) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test1/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test1(i)>0
                msg_txt=['-> Test 1 per type ' num2str(i) ' = ' num2str((type_test1(i)/ok_test1)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    if ok_test2==0
        msg_txt=['+> Test 2 (a, b and c) passed (number of connected Reaches downstream)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 2 (a:' num2str(ok_test2a) ', b:' num2str(ok_test2b) ' or c:' num2str(ok_test2c) ') failed (wrong number of connected Reaches downstream) ' num2str(ok_test2) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test2/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test2(i)>0
                msg_txt=['-> Test 2 per type ' num2str(i) ' = ' num2str((type_test2(i)/ok_test2)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================TEST 3 =======================================
    % Same as test 4 but for upstream Reaches
    % If a Reach B is connected and upstream of A, then A should be connected and downstream of Reach B. If false, We should
    % correct this by saying that A is downstream of B (we then suppose A<-B is true). If A is said to be upstream of B, then
    % maybe the link between A and B should be reversed (we then suppose B<-A is true), so A becomes upstream of B and B
    % downstream of A. This inversion is done according to opt_reverse_3b option (1 with some condition, or 2 always)
    % A Reach cannot be connected to itseft, we remove this link
    % A Reach A cannot have the same Reach B connected both upstream and downstream of it
    ok_test3a=0; % Case when a Reach B is upstream of A, but the reverse is not true, we complete but do not reverse A<-B
    ok_test3b=0; % Case when a Reach B is upstream of A, but the reverse is not true, and A is upstream of B, we do reverse to get B<-A
    ok_test3c=0; % Case when a Reach is upstream of itself A->A, we remove this link
    ok_test3d=0; % Case when a Reach B is both upstream and downstream of the tested one A (also done with test4d), we remove the link depending on the links indicated for B
    ok_test3e=0; % Case when a Reach B is connected upstream of a Reach A with B having no upstream Reach but 2 or more downstream Reaches
    type_test3(1:6)=zeros(1,6);
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        for j=1:n_rch_up_rch(i)
            id1=rch_id_up_rch(i,j); % jth Reach B id connected upstream of Reach A index i
            if id1==0
                % After Tests 1 & 2 this should not be possible, but below we can remove some links by putting some id=0, so this may happen there
                continue
            end
            index1=find(id_rch==id1,1,'first'); % Its index
            list_dn=rch_id_dn_rch(index1,1:n_rch_dn_rch(index1)); % ids of its downstream Reaches
            % We could use list_dn=rch_id_dn_rch(index1,:); in case n_rch_dn_rch(index1) is wrong
            if ~ismember(id_rch(i),list_dn)
                msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') is upstream of ' num2str(id_rch(i)) ' (' num2str(i) ') but the reverse is not true!'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>0 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                % We check if id_rch(i) is not in fact defined as upstream of index1
                list_up=rch_id_up_rch(index1,:); % ids of its upstream Reaches
                if ismember(id_rch(i),list_up)
                    msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is in fact defined as upstream of ' num2str(id1) ' (' num2str(index1) ') so it may be reversed, if opt_reverse_3b=' num2str(opt_reverse_3b) '>0!'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    if opt_reverse_3b==2 || (opt_reverse_3b==1 && n_rch_up_rch(index1)>=find(id_rch(i)==rch_id_up_rch(index1,:)))
                        case_reverse=1;
                        ok_test3b=ok_test3b+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=3.2;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=0;
                        type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                    else
                        case_reverse=0;
                        ok_test3a=ok_test3a+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=3.1;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=0;
                        type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                    end
                else
                    case_reverse=0;
                    ok_test3a=ok_test3a+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=3.1;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=0;
                    type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                end
                if opt_sword_corr_auto>0 && i_run>=i_run_min
                    if case_reverse==0
                        sub_case3="3a";
                        % Reach i becomes downstream of index1
                        k=find(rch_id_dn_rch(index1,:)==0,1,'first');
                        if ~isempty(k)
                            rch_id_dn_rch(index1,k)=id_rch(i);
                            % If it was also upstream we remove it
                            kk=find(id_rch(i)==rch_id_up_rch(index1,:));
                            if ~isempty(kk)
                                rch_id_up_rch(index1,kk)=0;
                            end
                        else
                            msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has already 4 downstream Reaches!'];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            msg_txt=['-> ' num2str(rch_id_dn_rch(index1,:))];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            if opt_pause>0
                                disp('Press a key to continue ...')
                                pause
                            end
                        end
                    else
                        sub_case3="3b";
                        % Reach j is no more upstream of Reach i
                        rch_id_up_rch(i,j)=0;
                        % Reach index1 becomes downstream of Reach i
                        k=find(rch_id_dn_rch(i,:)==0,1,'first');
                        if ~isempty(k)
                            rch_id_dn_rch(i,k)=id1;
                            % If it was also upstream we remove it
                            kk=find(id1==rch_id_up_rch(i,:));
                            if ~isempty(kk)
                                rch_id_up_rch(i,kk)=0;
                            end
                        else
                            msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has already 4 downstream Reaches!'];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            msg_txt=['-> ' num2str(rch_id_dn_rch(i,:))];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            if opt_pause>0
                                disp('Press a key to continue ...')
                                pause
                            end
                        end
                        % Reach i becomes upstream of index1
                        k=find(rch_id_up_rch(index1,:)==0,1,'first');
                        if ~isempty(k)
                            rch_id_up_rch(index1,k)=id_rch(i);
                            % If it was also downstream we remove it
                            kk=find(id_rch(i)==rch_id_dn_rch(index1,:));
                            if ~isempty(kk)
                                rch_id_dn_rch(index1,kk)=0;
                            end
                        else
                            msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has already 4 upstream Reaches!'];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            msg_txt=['-> ' num2str(rch_id_up_rch(index1,:))];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            if opt_pause>0
                                disp('Press a key to continue ...')
                                pause
                            end
                        end
                    end
                    rch_clean(i,2);
                    struct_reach=update_neighbor(struct_reach,i,sub_case3);
                    rch_clean(index1,2);
                    struct_reach=update_neighbor(struct_reach,index1,sub_case3);
                end
            end
        end
    end
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        nbij=n_rch_up_rch(i);
        for j=1:nbij
            id1=rch_id_up_rch(i,j); % jth Reach B id connected upstream of Reach A index i
            if isequal(id1,id_rch(i))
                msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') is upstream of itsef!'];
                disp(msg_txt)
                if opt_wrtlog>0 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test3c=ok_test3c+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=3.3;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                if opt_sword_corr_auto>0 && i_run>=i_run_min
                    % Reach j is no more upstream of Reach i
                    rch_id_up_rch(i,j)=0;
                    rch_clean(i,-1);
                    struct_reach=update_neighbor(struct_reach,i,"3c");
                end
            end
        end
    end
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        list_up=rch_id_up_rch(i,1:n_rch_up_rch(i)); % ids of its upstream Reaches
        list_dn=rch_id_dn_rch(i,1:n_rch_dn_rch(i)); % ids of its downstream Reaches
        list_up_dn=intersect(list_up,list_dn);
        if ~isempty(list_up_dn)
            msg_txt=['Reach ' num2str(list_up_dn) ' is both upstream and downstream of Reach ' num2str(id_rch(i)) ' (' num2str(i) ')!'];
            disp(msg_txt)
            ok_test3d=ok_test3d+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=3.4;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=0;
            type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
            if opt_sword_corr_auto>0 && i_run>=i_run_min
                for j=1:length(list_up_dn)
                    id1=list_up_dn(j); % The id of a concerned Reach
                    index1=find(id_rch==id1,1,'first'); % Its index
                    if ~ismember(i,rch_id_dn_rch(index1,1:n_rch_dn_rch(index1)))
                        k=find(rch_id_up_rch(i,:)==id1);
                        rch_id_up_rch(i,k)=0;
                    end
                    if ~ismember(i,rch_id_up_rch(index1,1:n_rch_up_rch(index1)))
                        k=find(rch_id_dn_rch(i,:)==id1);
                        rch_id_dn_rch(i,k)=0;
                    end
                end
                rch_clean(i,2);
                struct_reach=update_neighbor(struct_reach,i,"3d");
            end
        end
    end
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        nbij=n_rch_up_rch(i);
        for j=1:nbij
            id1=rch_id_up_rch(i,j); % jth Reach B id connected upstream of Reach A index i
            if id1==0
                % After Tests 1 & 2 this should not be possible, but below we can remove some links by putting some id=0, so this may happen there
                continue
            end
            index1=find(id_rch==id1,1,'first'); % Its index
            test_pb=0; % So far no problem
            if n_rch_dn_rch(index1)>1 % Several Reaches downstream
                if n_rch_up_rch(index1)==0
                    test_pb=1; % No Reach upstream
                else
                    for k=1:n_rch_up_rch(index1)
                        id2=rch_id_up_rch(index1,k);
                        index2=find(id_rch==id2,1,'first'); % Its index
                        if isempty(index2)
                            test_pb=3; % The upstream Reach is unknown
                        else
                            if n_rch_up_rch(index2)==0 && n_rch_dn_rch(index2)==1
                                test_pb=2; % The upstream Reach is not connected to any other Reach
                            end
                        end
                    end
                end
            end
            if test_pb>0
                if test_pb==1 && opt_warning_3e==1
                    msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has no upstream Reach but several downstream (' num2str(n_rch_dn_rch(index1)) '), this is suspicious!'];
                    disp(msg_txt)
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test3e=ok_test3e+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=3.5;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=0;
                    type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                elseif test_pb==2 && opt_warning_3e==1
                    msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has one upstream Reach (' num2str(id2) '), without any other connection but several downstream (' num2str(n_rch_dn_rch(index1)) '), this is suspicious!'];
                    disp(msg_txt)
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test3e=ok_test3e+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=3.5;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=0;
                    type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                elseif test_pb==3
                    msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has one upstream Reach (' num2str(id2) '), but it is unknown, this is a problem!'];
                    disp(msg_txt)
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test3e=ok_test3e+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=3.5;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=0;
                    type_test3(id_rch_t(i))=type_test3(id_rch_t(i))+1;
                end
                if opt_sword_corr_auto>0 && i_run>=i_run_min && opt_warning_3e==1
                    % We reverse connection between Reach i and index1 (i -> index1)
                    rch_id_up_rch(index1,1)=id_rch(i);
                    n_rch_up_rch(index1)=1;
                    % If it was also downstream we remove it
                    kk=find(id_rch(i)==rch_id_dn_rch(index1,:));
                    if ~isempty(kk)
                        rch_id_dn_rch(index1,kk)=0;
                        n_rch_dn_rch(i)=n_rch_dn_rch(i)-1;
                    end
                    k=find(rch_id_dn_rch(i,:)==0,1,'first');
                    rch_id_dn_rch(i,k)=id1;
                    n_rch_dn_rch(i)=n_rch_dn_rch(i)+1;
                    rch_id_up_rch(i,j)=0;
                    n_rch_up_rch(i)=n_rch_up_rch(i)-1;
                    rch_clean(i,2);
                    struct_reach=update_neighbor(struct_reach,i,"3e");
                    rch_clean(index1,-1);
                    struct_reach=update_neighbor(struct_reach,index1,"3e");
                    break
                end
            end
        end
    end
    ok_test3=ok_test3a+ok_test3b+ok_test3c+ok_test3d+ok_test3e;
    if ok_test3==0
        msg_txt=['+> Test 3 (a, b, c, d and e) passed (connectivity of Reaches upstream looks ok)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        % nb_case_test=nb_case_test+1;
        % list_case_test(nb_case_test,1)=i_run;
        % list_case_test(nb_case_test,2)=3.0;
        % list_case_test(nb_case_test,3)=i;
        msg_txt=['-> Test 3 (a:' num2str(ok_test3a) ', b:' num2str(ok_test3b) ', c:' num2str(ok_test3c) ', d:' num2str(ok_test3d) ' or e:' num2str(ok_test3e) ') failed (connectivity of Reaches upstream looks wrong) ' num2str(ok_test3) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test3/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test3(i)>0
                msg_txt=['-> Test 3 per type ' num2str(i) ' = ' num2str((type_test3(i)/ok_test3)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================TEST 4 =======================================
    % Same as test 3 but for downstream Reaches
    % If a Reach B is connected and downstream of A, then A should be connected and upstream of Reach B. If false, We should
    % correct this by saying that A is upstream of B (we then suppose A->B is true). If A is said to be downstream of B, then
    % maybe the link between A and B should be reversed (we then suppose B->A is true), so A becomes downstream of B and B
    % upstream of A. This inversion is done according to opt_reverse_4b option (1 with some condition, or 2 always)
    % A Reach cannot be connected to itseft, we remove this link
    % A Reach A cannot have the same Reach B connected both upstream and downstream of it
    ok_test4a=0; % Case when a Reach B is downstream of A, but the reverse is not true, we complete but do not reverse A->B
    ok_test4b=0; % Case when a Reach B is downstream of A, but the reverse is not true, we do reverse to get B->A
    ok_test4c=0; % Case when a Reach is downstream of itself A->A, we remove this link
    ok_test4d=0; % Case when a Reach B is both upstream and downstream of the tested one A (already done with test3d), we remove the link depending on the links indicated for B
    ok_test4e=0; % Case when a Reach B is connected downstream of a Reach A with B having no downstream Reach but 2 or more upstream Reaches
    type_test4(1:6)=zeros(1,6);
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        for j=1:n_rch_dn_rch(i)
            id1=rch_id_dn_rch(i,j); % jth Reach B id connected downstream of Reach A index i
            if id1==0
                % After Tests 1 & 2 this should not be possible, but below we can remove some links by putting some id=0, so this may happen there
                continue
            end
            index1=find(id_rch==id1,1,'first'); % Its index
            list_up=rch_id_up_rch(index1,1:n_rch_up_rch(index1)); % ids of its upstream Reaches
            % We could use list_up=rch_id_up_rch(index1,:); in case n_rch_up_rch(index1) is wrong
            if ~ismember(id_rch(i),list_up)
                msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') is downstream of ' num2str(id_rch(i)) ' (' num2str(i) ') but the reverse is not true!'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>0 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                % We check if id_rch(i) is not in fact defined as downstream of index1
                list_dn=rch_id_dn_rch(index1,:); % ids of its downstream Reaches
                if ismember(id_rch(i),list_dn)
                    msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is in fact defined as downstream of ' num2str(id1) ' (' num2str(index1) ') so it may be reversed, if opt_reverse_4b=' num2str(opt_reverse_4b) '>0!'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    if opt_reverse_4b==2 || (opt_reverse_4b==1 && n_rch_dn_rch(index1)>=find(id_rch(i)==rch_id_dn_rch(index1,:)))
                        case_reverse=1;
                        ok_test4b=ok_test4b+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=4.2;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=0;
                        type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                    else
                        case_reverse=0;
                        ok_test4a=ok_test4a+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=4.1;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=0;
                        type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                    end
                else
                    case_reverse=0;
                    ok_test4a=ok_test4a+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=4.1;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=0;
                    type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                end
                if opt_sword_corr_auto>0 && i_run>=i_run_min
                    if case_reverse==0
                        sub_case4="4a";
                        % Reach i becomes upstream of index1
                        k=find(rch_id_up_rch(index1,:)==0,1,'first');
                        if ~isempty(k)
                            rch_id_up_rch(index1,k)=id_rch(i);
                            % If it was also downstream we remove it
                            kk=find(id_rch(i)==rch_id_dn_rch(index1,:));
                            if ~isempty(kk)
                                rch_id_dn_rch(index1,kk)=0;
                            end
                        else
                            msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has already 4 upstream Reaches!'];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            msg_txt=['-> ' num2str(rch_id_up_rch(index1,:))];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            if opt_pause>0
                                disp('Press a key to continue ...')
                                pause
                            end
                        end
                    else
                        sub_case4="4b";
                        % Reach j is no more downstream of Reach i
                        rch_id_dn_rch(i,j)=0;
                        % Reach index1 becomes upstream of Reach i
                        k=find(rch_id_up_rch(i,:)==0,1,'first');
                        if ~isempty(k)
                            rch_id_up_rch(i,k)=id1;
                            % If it was also downstream we remove it
                            kk=find(id1==rch_id_dn_rch(i,:));
                            if ~isempty(kk)
                                rch_id_dn_rch(i,kk)=0;
                            end
                        else
                            msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has already 4 upstream Reaches!'];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            msg_txt=['-> ' num2str(rch_id_up_rch(i,:))];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            if opt_pause>0
                                disp('Press a key to continue ...')
                                pause
                            end
                        end
                        % Reach i becomes downstream of index1
                        k=find(rch_id_dn_rch(index1,:)==0,1,'first');
                        if ~isempty(k)
                            rch_id_dn_rch(index1,k)=id_rch(i);
                            % If it was also upstream we remove it
                            kk=find(id_rch(i)==rch_id_up_rch(index1,:));
                            if ~isempty(kk)
                                rch_id_up_rch(index1,kk)=0;
                            end
                        else
                            msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has already 4 downstream Reaches!'];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            msg_txt=['-> ' num2str(rch_id_dn_rch(index1,:))];
                            disp(msg_txt)
                            if opt_wrtlog>0 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            if opt_pause>0
                                disp('Press a key to continue ...')
                                pause
                            end
                        end
                    end
                    rch_clean(i,2);
                    struct_reach=update_neighbor(struct_reach,i,sub_case4);
                    rch_clean(index1,2);
                    struct_reach=update_neighbor(struct_reach,index1,sub_case4);
                end
            end
        end
    end
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        nbij=n_rch_dn_rch(i);
        for j=1:nbij
            id1=rch_id_dn_rch(i,j); % jth Reach B id connected downstream of Reach A index i
            if isequal(id1,id_rch(i))
                index1=find(id_rch==id1,1,'first'); % Its index
                msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') is downstream of itsef! (' num2str(i) ')'];
                disp(msg_txt)
                if opt_wrtlog>0 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test4c=ok_test4c+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=4.3;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                if opt_sword_corr_auto>0 && i_run>=i_run_min
                    % Reach j is no more downstream of Reach i
                    rch_id_dn_rch(i,j)=0;
                    rch_clean(i,1);
                    struct_reach=update_neighbor(struct_reach,i,"4c");
                end
            end
        end
    end
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        list_up=rch_id_up_rch(i,1:n_rch_up_rch(i)); % ids of its upstream Reaches
        list_dn=rch_id_dn_rch(i,1:n_rch_dn_rch(i)); % ids of its downstream Reaches
        list_up_dn=intersect(list_up,list_dn);
        if ~isempty(list_up_dn)
            msg_txt=['Reach ' num2str(list_up_dn) ' is both upstream and downstream of Reach ' num2str(id_rch(i)) ' (' num2str(i) ')!'];
            disp(msg_txt)
            if opt_wrtlog>0 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            ok_test4d=ok_test4d+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=4.4;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=0;
            type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
            if opt_sword_corr_auto>0 && i_run>=i_run_min
                for j=1:length(list_up_dn)
                    id1=list_up_dn(j); % The id of a concerned Reach
                    index1=find(id_rch==id1,1,'first'); % Its index
                    if ~ismember(i,rch_id_up_rch(index1,1:n_rch_up_rch(index1)))
                        k=find(rch_id_dn_rch(i,:)==id1);
                        rch_id_dn_rch(i,k)=0;
                    end
                    if ~ismember(i,rch_id_dn_rch(index1,1:n_rch_dn_rch(index1)))
                        k=find(rch_id_up_rch(i,:)==id1);
                        rch_id_up_rch(i,k)=0;
                    end
                end
                rch_clean(i,2);
                struct_reach=update_neighbor(struct_reach,i,"4d");
            end
        end
    end
    for i=1:nb_rch % Reach A
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        nbij=n_rch_dn_rch(i);
        for j=1:nbij
            id1=rch_id_dn_rch(i,j); % jth Reach B id connected downstream of Reach A index i
            if id1==0
                % After Tests 1 & 2 this should not be possible, but below we can remove some links by putting some id=0, so this may happen there
                continue
            end
            index1=find(id_rch==id1,1,'first'); % Its index
            test_pb=0; % So far no problem
            if n_rch_up_rch(index1)>1 % Several Reaches upstream
                if n_rch_dn_rch(index1)==0
                    test_pb=1; % No Reach downstream
                else
                    for k=1:n_rch_dn_rch(index1)
                        id2=rch_id_dn_rch(index1,k);
                        index2=find(id_rch==id2,1,'first'); % Its index
                        if isempty(index2)
                            test_pb=3; % The upstream Reach is unknown
                        else
                            if n_rch_dn_rch(index2)==0 && n_rch_up_rch(index2)==1
                                test_pb=2; % The downstream Reach is not connected to any other Reach
                            end
                        end
                    end
                end
            end
            if test_pb>0
                if test_pb==1 && opt_warning_4e==1
                    msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has no downstream Reach but several upstream (' num2str(n_rch_up_rch(index1)) '), this is suspicious!'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test4e=ok_test4e+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=4.5;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=test_pb;
                    type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                elseif test_pb==2 && opt_warning_4e==1
                    msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has one downstream Reach (' num2str(id2) '), without any other connection but several upstream (' num2str(n_rch_up_rch(index1)) '), this is suspicious!'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test4e=ok_test4e+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=4.5;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=test_pb;
                    type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                elseif test_pb==3
                    msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') has one downstream Reach (' num2str(id2) '), but it is unknown, this is a problem!'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>0 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test4e=ok_test4e+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=4.5;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=test_pb;
                    type_test4(id_rch_t(i))=type_test4(id_rch_t(i))+1;
                end
                if opt_sword_corr_auto>0 && i_run>=i_run_min && opt_warning_4e==1
                    % We reverse connection between Reach i and index1 (index1 -> i)
                    rch_id_dn_rch(index1,1)=id_rch(i);
                    n_rch_dn_rch(index1)=1;
                    % If it was also upstream we remove it
                    kk=find(id_rch(i)==rch_id_up_rch(index1,:));
                    if ~isempty(kk)
                        rch_id_up_rch(index1,kk)=0;
                        n_rch_up_rch(i)=n_rch_up_rch(i)-1;
                    end
                    k=find(rch_id_up_rch(i,:)==0,1,'first');
                    rch_id_up_rch(i,k)=id1;
                    n_rch_up_rch(i)=n_rch_up_rch(i)+1;
                    rch_id_dn_rch(i,j)=0;
                    n_rch_dn_rch(i)=n_rch_dn_rch(i)-1;
                    rch_clean(i,2);
                    struct_reach=update_neighbor(struct_reach,i,"4e");
                    rch_clean(index1,1);
                    struct_reach=update_neighbor(struct_reach,index1,"4e");
                    break
                end
            end
        end
    end
    ok_test4=ok_test4a+ok_test4b+ok_test4c+ok_test4d+ok_test4e;
    if ok_test4==0
        msg_txt=['+> Test 4 (a, b, c, d and e) passed (connectivity of Reaches downstream looks ok)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        % nb_case_test=nb_case_test+1;
        % list_case_test(nb_case_test,1)=i_run;
        % list_case_test(nb_case_test,2)=4.0;
        % list_case_test(nb_case_test,3)=i;
        msg_txt=['-> Test 4 (a:' num2str(ok_test4a) ', b:' num2str(ok_test4b) ', c:' num2str(ok_test4c) ', d:' num2str(ok_test4d) ' or e:' num2str(ok_test4e) ') failed (connectivity of Reaches downstream looks wrong) ' num2str(ok_test4) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test4/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test4(i)>0
                msg_txt=['-> Test 4 per type ' num2str(i) ' = ' num2str((type_test4(i)/ok_test4)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================TEST 5 =======================================
    % A Reach should be connected to at least 1 other Reach either upstream or downstream (Test 5a)
    % If 2 Reaches A & B are connected either upstream or downstream to a same one C, then A & B do not need to be personally
    % connected (Test 5b)
    ok_test5a=0;
    ok_test5b=0;
    type_test5a(1:6)=zeros(1,6);
    type_test5b(1:6)=zeros(1,6);
    % Test 5a
    for i=1:nb_rch
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        if n_rch_up_rch(i)==0 && n_rch_dn_rch(i)==0
            msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is not connected to any other Reach!'];
            disp(msg_txt)
            if opt_wrtlog>0 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            ok_test5a=ok_test5a+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=5.1;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=0;
            type_test5a(id_rch_t(i))=type_test5a(id_rch_t(i))+1;
            if ok_correct_sword_other==1
                patch_comment=['Lonely Reach'];
                correct_sword_other(id_rch(i),patch_comment);
            end
            if opt_pause>1
                disp('Press a key to continue ...')
                pause
            end
        end
    end
    % Test 5b
    for i=1:nb_rch
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        list_up=rch_id_up_rch(i,1:n_rch_up_rch(i)); % ids of its upstream Reaches
        list_dn=rch_id_dn_rch(i,1:n_rch_dn_rch(i)); % ids of its downstream Reaches
        list_co=union(list_up,list_dn); % ids of its connected Reaches (up & dn)
        list_co=list_co(find(list_co>0));
        for j=1:length(list_co)
            % We check all Reaches connected to Reach i
            index1=find(id_rch==list_co(j),1,'first'); % Its index
            list_up1=rch_id_up_rch(index1,1:n_rch_up_rch(index1)); % ids of its upstream Reaches
            list_dn1=rch_id_dn_rch(index1,1:n_rch_dn_rch(index1)); % ids of its downstream Reaches
            list_co1=union(list_up1,list_dn1); % ids of its connected Reaches (up & dn)
            list_co1=list_co1(find(list_co1>0));
            if ~isempty(intersect(list_co,list_co1))
                msg_txt=['When studying Reach ' num2str(id_rch(i)) ' : Reaches ' num2str(list_co(j)) ' and ' num2str(intersect(list_co,list_co1)) ' are connected directly but may not, since already connected either upstream or downstream of ' num2str(id_rch(i))];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>0 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test5b=ok_test5b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=5.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test5b(id_rch_t(i))=type_test5b(id_rch_t(i))+1;
                if ok_correct_sword_other==1
                    patch_comment=['Unnecessary connection between Reaches ' num2str(list_co(j)) ' and ' num2str(intersect(list_co,list_co1))];
                    correct_sword_other(id_rch(i),patch_comment);
                end
                if opt_pause>1
                    disp('Press a key to continue ...')
                    pause
                end
            end
        end
    end
    if ok_test5a==0
        msg_txt=['+> Test 5a passed (no single Reaches)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 5a failed (single Reaches) ' num2str(ok_test5a) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test5a/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test5a(i)>0
                msg_txt=['-> Test 5a per type ' num2str(i) ' = ' num2str((type_test5a(i)/ok_test5a)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    if ok_test5b==0
        msg_txt=['+> Test 5b passed (no unnecessary connection between Reaches)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 5b failed (unnecessary connection between Reaches) ' num2str(ok_test5b) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test5b/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test5b(i)>0
                msg_txt=['-> Test 5b per type ' num2str(i) ' = ' num2str((type_test5b(i)/ok_test5b)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================Update of Reach connectivity==================
    % ===========================In Automatic Mode Correction==================
    if opt_sword_corr_auto>0 && i_run>=i_run_min
        corrected_5=0;
        ii=length(struct_reach);
        for j=1:ii
            i1=find(rch_id_up_rch(struct_reach(j).reach,:)>0);
            r1=rch_id_up_rch(struct_reach(j).reach,i1);
            if isempty(r1)
                r1=0;
            end
            i2=find(rch_id_dn_rch(struct_reach(j).reach,:)>0);
            r2=rch_id_dn_rch(struct_reach(j).reach,i2);
            if isempty(r2)
                r2=0;
            end
            for i=1:length(struct_reach(j).code)
                if i==1
                    patch_comment="Test "+struct_reach(j).code(i);
                else
                    patch_comment=[patch_comment+", "+struct_reach(j).code(i)];
                end
            end
            disp(['Automatic correction: correct_sword_reach_neighbor(' num2str(id_rch(struct_reach(j).reach)) ',[' num2str(r1) '],[' num2str(r2) ']) : ' char(patch_comment)])
            correct_sword_reach_neighbor(id_rch(struct_reach(j).reach),r1,r2,patch_comment);
            corrected_5=corrected_5+1;
        end
        if corrected_5>0
            msg_txt=['-> Test 1-5 : ' num2str(corrected_5) ' automatic corrections suggested in the csv and json files'];
            disp(msg_txt)
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        end
    end
    % ===========================End Automatic Mode Correction=================
    % ===========================In Automatic Mode Correction==================
    %
    % ===========================TEST 6 =======================================
    % The distance from the geolocations of connected Reaches should be around or less than 10-20 km : we take eps=30 km
    % The distance from the geolocations of Nodes inside a Reach should be around 200 m : we take eps=400 m
    ok_test6a=0;
    ok_test6b=0;
    dist_epsa=30000; % Seuil en m pour les Reaches
    dist_epsb=400; % Seuil en m pour les Nodes
    dist_maxa=0;
    dist_maxb=0;
    type_test6a(1:6)=zeros(1,6);
    type_test6b(1:6)=zeros(1,6);
    for i=1:nb_rch
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        for j=1:n_rch_up_rch(i)
            id1=rch_id_up_rch(i,j); % jth Reach id connected upstream of Reach index i
            index1=find(id_rch==id1,1,'first'); % Its index
            if isempty(index1)
                continue
            end
            dist=lldistkm([y_rch(i) x_rch(i)],[y_rch(index1) x_rch(index1)])*1000; % Distance between connected Reaches up
            dist_maxa=max(dist_maxa,dist);
            if dist>dist_epsa
                msg_txt=['Connected Reaches ' num2str(id1) ' (' num2str(index1) ') and ' num2str(id_rch(i)) ' (' num2str(i) ') are distant about ' num2str(dist/1000,'%.2f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test6a=ok_test6a+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=6.1;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=dist;
                type_test6a(id_rch_t(i))=type_test6a(id_rch_t(i))+1;
                if ok_correct_sword_other==1
                    patch_comment=msg_txt;
                    correct_sword_other(id_rch(i),patch_comment);
                end
            end
        end
        for j=1:n_rch_dn_rch(i)
            id1=rch_id_dn_rch(i,j); % jth Reach id connected downstream of Reach index i
            index1=find(id_rch==id1,1,'first'); % Its index
            if isempty(index1)
                continue
            end
            dist=lldistkm([y_rch(i) x_rch(i)],[y_rch(index1) x_rch(index1)])*1000; % Distance between connected Reaches dn
            dist_maxa=max(dist_maxa,dist);
            if dist>dist_epsa
                msg_txt=['Connected Reaches ' num2str(id1) ' (' num2str(index1) ') and ' num2str(id_rch(i)) ' (' num2str(i) ') are distant about ' num2str(dist/1000,'%.2f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test6a=ok_test6a+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=6.1;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=dist;
                type_test6a(id_rch_t(i))=type_test6a(id_rch_t(i))+1;
                if ok_correct_sword_other==1
                    patch_comment=msg_txt;
                    correct_sword_other(id_rch(i),patch_comment);
                end
            end
        end
        for j=index_rch_node(i,1):index_rch_node(i,2)
            if id_nod_rch(j)~=id_rch(i)
                % The Node j is not in the Reach i
                continue
            end
            % On cherche le Node dont l'ID est juste après, donc juste à l'amont
            % On teste sur les ID et non plus sur les index (POM 03/04/25)
            % On ajoute 10 et pas 1 car il y a le dernier chiffre pour le type
            % j2=find(id_nod(index_rch_node(i,1):(index_rch_node(i,2)))==(id_nod(j)+10));
            % if ~isempty(j2)
            %     j2=index_rch_node(i,1)+j2-1;
            % else
            %     continue
            % end
            % if id_nod_rch(j2)~=id_rch(i)
            %     % The Node j2 is not in the Reach i
            %     continue
            % end
            j2=find_node(i,j,'aft');
            if isempty(j2)
                continue
            end
            if exist('main_side_nod','var')
                if main_side_nod(j)>opt_warning_6b || main_side_nod(j2)>opt_warning_6b
                    continue
                end
            end
            dist=lldistkm([y_nod(j) x_nod(j)],[y_nod(j2) x_nod(j2)])*1000; % Distance between the 2 consecutive Nodes
            dist_maxb=max(dist_maxb,dist);
            if dist>dist_epsb
                msg_txt=['Contiguous Nodes ' num2str(id_nod(j)) ' (' num2str(j) ') and ' num2str(id_nod(j2)) ' (' num2str(j2) ') are distant about ' num2str(dist/1000,'%.2f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test6b=ok_test6b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=6.2;
                list_case_test(nb_case_test,3)=j;
                list_case_test(nb_case_test,4)=dist;
                type_test6b(id_rch_t(i))=type_test6b(id_rch_t(i))+1;
                if ok_correct_sword_other==1
                    patch_comment=msg_txt;
                    correct_sword_other(id_rch(i),patch_comment);
                end
            end
        end
    end
    if ok_test6a==0
        msg_txt=['+> Test 6a passed (distance between connected Reaches looks ok, max=' num2str(dist_maxa/1000,'%.2f') ' km)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 6a failed (distance between connected Reaches looks wrong) ' num2str(ok_test6a) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test6a/nb_rch_verified)*100,'%.3f') '%). More than ' num2str(dist_epsa/1000,'%.2f') ' km, up to ' num2str(dist_maxa/1000,'%.2f') ' km'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test6a(i)>0
                msg_txt=['-> Test 6a per type ' num2str(i) ' = ' num2str((type_test6a(i)/ok_test6a)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    if ok_test6b==0
        msg_txt=['+> Test 6b passed (distance between contiguous Nodes looks ok, max=' num2str(dist_maxb/1000,'%.2f') ' km)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 6b failed (distance between contiguous Nodes looks wrong) ' num2str(ok_test6b) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test6b/nb_nod_verified)*100,'%.3f') '%). More than ' num2str(dist_epsb/1000,'%.2f') ' km, up to ' num2str(dist_maxb/1000,'%.2f') ' km'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        for i=1:6
            if type_test6b(i)>0
                msg_txt=['-> Test 6b per type ' num2str(i) ' = ' num2str((type_test6b(i)/ok_test6b)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================TEST 7 =======================================
    % Reaches ID are numbered from downstream to upstream when connected, so dist_out should increase accordingly (test7a)
    % We also check that dist_out do not change too much to be true (test7b)
    ok_test7a=0;
    ok_test7b=0;
    type_test7a(1:6)=zeros(1,6);
    type_test7b(1:6)=zeros(1,6);
    length_max_node=300;
    % length_max_reach=20000;
    length_max_reach=30000; % On passe à 30 km pour déclancher moins de warnings
    % length_max_reach=40000; % On passe à 40 km pour déclancher moins de warnings
    gap_length_min_node=50;
    gap_length_min_reach=500;
    change_dist_out=0;
    change_dist_out_max=0;
    for i=1:nb_rch
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        % We limit this test for Reaches whose main_side_rch(i)<=opt_warning_7 (0 for main network, 1 for side network, 2 for secondary outlet)
        if exist('main_side_rch','var')
            if main_side_rch(i)>opt_warning_7
                continue
            end
        end
        for j=1:n_rch_up_rch(i)
            id1=rch_id_up_rch(i,j); % jth Reach id connected upstream of Reach index i
            index1=find(id_rch==id1,1,'first'); % Its index
            if isempty(index1)
                continue
            end
            if exist('main_side_rch','var')
                if main_side_rch(index1)>opt_warning_7
                    continue
                end
            end
            if dist_out_rch(i)>dist_out_rch(index1)
                msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') is upstream of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') but its dist_out is smaller: ' num2str(dist_out_rch(index1)/1000,'%.2f') ' < ' num2str(dist_out_rch(i)/1000,'%.2f') ' km (gap=' num2str((dist_out_rch(index1)-dist_out_rch(i))/1000,'%.2f') ' km)'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test7a=ok_test7a+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=7.1;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=dist_out_rch(index1)-dist_out_rch(i);
                type_test7a(id_rch_t(i))=type_test7a(id_rch_t(i))+1;
            end
            change_dist_out=abs(dist_out_rch(i)-dist_out_rch(index1));
            if change_dist_out>length_max_reach
                msg_txt=['Dist_out between Reach ' num2str(id1) ' (' num2str(index1) ') and Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is changing a lot: ' num2str(change_dist_out/1000,'%.2f') ' km [' num2str(dist_out_rch(index1)/1000,'%.2f') '->' num2str(dist_out_rch(i)/1000,'%.2f') ']'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test7b=ok_test7b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=7.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=dist_out_rch(i)-dist_out_rch(index1);
                type_test7b(id_rch_t(i))=type_test7b(id_rch_t(i))+1;
                change_dist_out_max=max(change_dist_out_max,change_dist_out);
            end
        end
        for j=1:n_rch_dn_rch(i)
            id1=rch_id_dn_rch(i,j); % jth Reach id connected downstream of Reach index i
            index1=find(id_rch==id1,1,'first'); % Its index
            if isempty(index1)
                continue
            end
            if exist('main_side_rch','var')
                if main_side_rch(index1)>opt_warning_7
                    continue
                end
            end
            if dist_out_rch(i)<dist_out_rch(index1)
                msg_txt=['Reach ' num2str(id1) ' (' num2str(index1) ') is downstream of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') but its dist_out is larger: ' num2str(dist_out_rch(index1)/1000,'%.2f') ' > ' num2str(dist_out_rch(i)/1000,'%.2f') ' km (gap=' num2str((dist_out_rch(index1)-dist_out_rch(i))/1000,'%.2f') ' km)'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test7a=ok_test7a+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=7.1;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=dist_out_rch(index1)-dist_out_rch(i);
                type_test7a(id_rch_t(i))=type_test7a(id_rch_t(i))+1;
            end
            change_dist_out=abs(dist_out_rch(i)-dist_out_rch(index1));
            if change_dist_out>length_max_reach
                msg_txt=['Dist_out between Reach ' num2str(id1) ' (' num2str(index1) ') and Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is changing a lot: ' num2str(change_dist_out/1000,'%.2f') ' km [' num2str(dist_out_rch(index1)/1000,'%.2f') '->' num2str(dist_out_rch(i)/1000,'%.2f') ']'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test7b=ok_test7b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=7.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=dist_out_rch(i)-dist_out_rch(index1);
                type_test7b(id_rch_t(i))=type_test7b(id_rch_t(i))+1;
                change_dist_out_max=max(change_dist_out_max,change_dist_out);
            end
        end
    end
    if ok_test7a==0
        msg_txt=['+> Test 7a passed (dist_out is increasing upstream for Reaches)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 7a failed (dist_out is not increasing upstream for Reaches) ' num2str(ok_test7a) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test7a/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test7a(i)>0
                msg_txt=['-> Test 7a per type ' num2str(i) ' = ' num2str((type_test7a(i)/ok_test7a)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    if ok_test7b==0
        msg_txt=['+> Test 7b passed (dist_out for Reaches is increasing reasonably, less than ' num2str(length_max_reach/1000,'%.2f') ' km)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 7b failed (dist_out for Reaches is increasing maybe too much) ' num2str(ok_test7b) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test7b/nb_rch_verified)*100,'%.3f') '%). More than ' num2str(length_max_reach/1000,'%.2f') ' km, up to ' num2str(change_dist_out_max/1000,'%.2f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test7b(i)>0
                msg_txt=['-> Test 7b per type ' num2str(i) ' = ' num2str((type_test7b(i)/ok_test7b)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================TEST 8, 9 & 10 ===============================
    % Indexes of the Nodes of the Reach should be correct (first <= last). This table (index_rch_node) is calculated by
    % sword_compute for the boost mode (Test 8a)
    % So this is NOT a sword table, but it is calculated from several sword tables (id_nod_ctl used in index_nod_ctl also a
    % boost mode table, and id_rch_ctl), or directly (better option but slower)
    % Some validy tests of these tables are then already done in sword_compute
    % Number of Nodes of the Reach should be correct (Test 8b)
    % Allocation of Nodes inside a Reach should be correct (Test 9a)
    % Check if indexes of Nodes inside a Reach are continuous (not error, just warning) (Test 9b)
    % Allocation of Centerline Points inside a Reach should be correct (Test 9c)
    % Allocation of Centerline Points inside a Node should be correct (Test 9d)
    % Nodes ID are numbered from downstream to upstream, so dist_out should increase accordingly (Test 10a)
    % Dist_out should not increase too much (ex: <400m, 600m) between 2 Nodes (Test 10b)
    % Same as Test 10a but between connected Reaches up or down (Test 10c)
    % Same as Test 10b but between connected Reaches up or down (Test 10d)
    % We can do these tests only if index_rch_node has been calculated, so if opt_sword_comp~=0
    if opt_sword_comp~=0
        ok_test8a=0; % Nodes indexes inversed
        ok_test8b=0; % mismatch in number of Nodes inside its Reach
        ok_test9a=0; % position of a Node not inside the good Reach
        ok_test9b=0; % not continuous Node indexes inside the Reach
        ok_test9c=0; % position of a Centerline Point not inside the good Reach
        ok_test9d=0; % position of a Centerline Point not inside the good Node
        ok_test10a=0; % dist_out increasing for a downstream Node inside a Reach
        ok_test10b=0; % dist_out change too large between 2 consecutive Nodes inside a Reach
        ok_test10c=0; % Same as Test 10a but between connected Reaches up or down
        ok_test10d=0; % Same as Test 10b but between connected Reaches up or down
        type_test8a(1:6)=zeros(1,6);
        type_test8b(1:6)=zeros(1,6);
        type_test9a(1:6)=zeros(1,6);
        type_test9b(1:6)=zeros(1,6);
        type_test9c(1:6)=zeros(1,6);
        type_test9d(1:6)=zeros(1,6);
        type_test10a(1:6)=zeros(1,6);
        type_test10b(1:6)=zeros(1,6);
        type_test10c(1:6)=zeros(1,6);
        type_test10d(1:6)=zeros(1,6);
        % length_max_node=400;
        length_max_node=600; % On passe à 600 m pour déclancher moins de warnings
        change_dist_out=0;
        change_dist_out_maxb=0;
        change_dist_out_maxd=0;
        step_percent=0.1;
        step_value=floor(nb_rch*step_percent);
        lapstime=tic;
        disp('Starting Tests 8, 9 & 10 ...')
        for i=1:nb_rch
            if opt_filter_validity>0 && ~ismember(i,ref_rch)
                continue
            end
            if ~id_rch_filter(i)
                continue
            end
            if index_rch_node(i,2)<index_rch_node(i,1)
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has its Nodes indexes inversed! Last=' num2str(index_rch_node(i,2)) ' < First=' num2str(index_rch_node(i,1)) '!'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test8a=ok_test8a+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=8.1;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test8a(id_rch_t(i))=type_test8a(id_rch_t(i))+1;
            end
            if n_rch_nod(i)~=index_rch_node(i,2)-index_rch_node(i,1)+1-index_rch_node(i,3)
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is supposed to have ' num2str(n_rch_nod(i)) ' Nodes, but seems to rather have ' num2str(abs(index_rch_node(i,2)-index_rch_node(i,1)+1-index_rch_node(i,3))) ' ones!'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test8b=ok_test8b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=8.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=n_rch_nod(i)-(index_rch_node(i,2)-index_rch_node(i,1)+1-index_rch_node(i,3));
                type_test8b(id_rch_t(i))=type_test8b(id_rch_t(i))+1;
            end
            if index_rch_node(i,3)==0
                for j=index_rch_node(i,1):index_rch_node(i,2)
                    if id_nod_rch(j)~=id_rch(i)
                        index1=find(id_rch==id_nod_rch(j),1,'first'); % Its index
                        msg_txt=['Node ' num2str(id_nod(j)) ' (' num2str(j) ') is placed in Reach ' num2str(id_rch(i)) ' (' num2str(i) '), but is said to be in ' num2str(id_nod_rch(j)) ' (' num2str(index1) ')'];
                        if opt_disp_details>0
                            disp(msg_txt)
                        end
                        if opt_wrtlog>1 && opt_wrtlog_details>0
                            fprintf(fidLog,'%s\r\n',msg_txt);
                        end
                        ok_test9a=ok_test9a+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=9.1;
                        list_case_test(nb_case_test,3)=j;
                        list_case_test(nb_case_test,4)=0;
                        type_test9a(id_rch_t(i))=type_test9a(id_rch_t(i))+1;
                    end
                end
            else
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has some Nodes with not-continuous indexes: ' num2str(index_rch_node(i,3)) ' such Nodes'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test9b=ok_test9b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=9.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test9b(id_rch_t(i))=type_test9b(id_rch_t(i))+1;
            end
            % Test 9c
            new_pb_9c=0; % We will count this pb only once per Reach
            i1=id_rch_ctl(i,1); % minimum high-resolution centerline point ids along each reach
            i2=id_rch_ctl(i,2); % maximum high-resolution centerline point ids along each reach
            for j=i1:i2
                case_9c=0; % So far so good
                k=find(id_ctl==j); % On cherche l'index du centerline point j
                if length(k)==0
                    % Le ctl id j n'existe pas
                    case_9c=1;
                elseif length(k)>1
                    % Le ctl id j existe plusieurs fois
                    case_9c=2;
                    k=k(1); % On ne prend que le premier
                end
                if length(k)==1
                    kk=find(id_ctl_rch(k,:)==id_rch(i)); % Cherche si le Reach id_rch(i) est bien indiqué comme hote de ce centerline point
                    if length(kk)==0 % On peut avoir de 1 à 4 Reaches hote de ce centerline point
                        % Le ctl index k (id j) n'est pas rattaché au Reach id_rch(i)
                        case_9c=3;
                    end
                end
                if case_9c>0
                    if opt_disp_details>0 || (opt_wrtlog>1 && opt_wrtlog_details>0)
                        if case_9c==1
                            msg_txt=['Centerline Point ' num2str(j) ' placed in Reach ' num2str(id_rch(i)) ' (' num2str(i) '), is not found (case_9c=' num2str(case_9c) ')'];
                        elseif case_9c==2
                            msg_txt=['Centerline Point ' num2str(j) ' (' num2str(k) ') placed in Reach ' num2str(id_rch(i)) ' (' num2str(i) '), is found many times (case_9c=' num2str(case_9c) ')'];
                        elseif case_9c==3
                            msg_txt=['Centerline Point ' num2str(j) ' (' num2str(k) ') is placed in Reach ' num2str(id_rch(i)) ' (' num2str(i) '), but is said to be in ' num2str(id_ctl_rch(k,1)) ' (case_9c=' num2str(case_9c) ')'];
                        end
                        if opt_disp_details>0
                            disp(msg_txt)
                        end
                        if opt_wrtlog>1 && opt_wrtlog_details>0
                            fprintf(fidLog,'%s\r\n',msg_txt);
                        end
                    end
                    if new_pb_9c==0
                        ok_test9c=ok_test9c+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=9.3;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=j+case_9c/10; % On met l'id du ctl point plus en décimale le type de pb
                        type_test9c(id_rch_t(i))=type_test9c(id_rch_t(i))+1;
                        new_pb_9c=1;
                        if ~(opt_disp_details>0 || (opt_wrtlog>1 && opt_wrtlog_details>0))
                            % Useless to continue since no msg_txt nor list_case_test will be done for next ctl points
                            break
                        end
                    end
                end
            end
            % Test 9d
            list_n=find(id_nod_rch==id_rch(i));
            % n1=length(list_n); % Nombre de noeuds du Reach id_rch(i)
            % if n_rch_nod(i)~=n1
            %     % Le nombre de noeud du Reach n'est pas bon
            %     disp('Press a key to continue ...')
            %     pause
            % end
            for ii=1:length(list_n)
                new_pb_9d=0; % We will count this pb only once per Node (first problematic Centerline Point of this Node)
                % Boucle sur les noeuds du Reach
                i3=list_n(ii); % Index du Node
                i1=id_nod_ctl(i3,1); % minimum high-resolution centerline point ids along each node
                i2=id_nod_ctl(i3,2); % maximum high-resolution centerline point ids along each node
                for j=i1:i2
                    case_9d=0; % So far so good
                    k=find(id_ctl==j); % On cherche l'index du centerline point j
                    if length(k)==0
                        % Le ctl id j n'existe pas
                        case_9d=1;
                    elseif length(k)>1
                        % Le ctl id j existe plusieurs fois
                        case_9d=2;
                        k=k(1); % On ne prend que le premier
                    end
                    if length(k)==1
                        kk=find(id_ctl_nod(k,:)==id_nod(i3)); % Cherche si le Node id_nod(i3) est bien indiqué comme hote de ce centerline point
                        if length(kk)==0
                            % Le ctl index k (id j) n'est pas rattaché au Node id_nod(i3)
                            case_9d=3;
                        end
                    end
                    if case_9d>0
                        if opt_disp_details>0 || (opt_wrtlog>1 && opt_wrtlog_details>0)
                            if case_9d==1
                                msg_txt=['Centerline Point ' num2str(j) ' placed in Node ' num2str(id_nod(i3)) ' (' num2str(i3) '), is not found (case_9d=' num2str(case_9d) ')'];
                            elseif case_9d==2
                                msg_txt=['Centerline Point ' num2str(j) ' (' num2str(k) ') placed in Node ' num2str(id_nod(i3)) ' (' num2str(i3) '), is found many times (case_9d=' num2str(case_9d) ')'];
                            elseif case_9d==3
                                msg_txt=['Centerline Point ' num2str(j) ' (' num2str(k) ') is placed in Node ' num2str(id_nod(i3)) ' (' num2str(i3) '), but is said to be in ' num2str(id_ctl_nod(k,1)) ' (case_9d=' num2str(case_9d) ')'];
                            end
                        end
                        if opt_disp_details>0
                            disp(msg_txt)
                        end
                        if opt_wrtlog>1 && opt_wrtlog_details>0
                            fprintf(fidLog,'%s\r\n',msg_txt);
                        end
                        if new_pb_9d==0
                            ok_test9d=ok_test9d+1;
                            nb_case_test=nb_case_test+1;
                            list_case_test(nb_case_test,1)=i_run;
                            list_case_test(nb_case_test,2)=9.4;
                            list_case_test(nb_case_test,3)=i3;
                            list_case_test(nb_case_test,4)=j+case_9d/10; % On met l'id du ctl point plus en décimale le type de pb
                            type_test9d(id_rch_t(i))=type_test9d(id_rch_t(i))+1;
                            new_pb_9d=1;
                            if ~(opt_disp_details>0 || (opt_wrtlog>1 && opt_wrtlog_details>0))
                                % Useless to continue since no msg_txt nor list_case_test will be done for next ctl points
                                break
                            end
                        end
                    end
                end
            end
            % Test 10a & 10b
            for j=index_rch_node(i,1):index_rch_node(i,2)
                if id_nod_rch(j)~=id_rch(i)
                    % The Node j is not in the Reach i
                    continue
                end
                % On cherche le Node dont l'ID est juste après, donc juste à l'amont
                % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % On ajoute 10 et pas 1 car il y a le dernier chiffre pour le type
                % j2=find(id_nod(index_rch_node(i,1):(index_rch_node(i,2)))==(id_nod(j)+10));
                % if ~isempty(j2)
                %     j2=index_rch_node(i,1)+j2-1;
                % else
                %     continue
                % end
                % if id_nod_rch(j2)~=id_rch(i)
                %     % The Node j2 is not in the Reach i
                %     continue
                % end
                j2=find_node(i,j,'aft');
                if isempty(j2)
                    continue
                end
                if exist('main_side_nod','var')
                    if main_side_nod(j)>opt_warning_10ab || main_side_nod(j2)>opt_warning_10ab
                        continue
                    end
                end
                if dist_out_nod(j2)<dist_out_nod(j)
                    msg_txt=['Node ' num2str(id_nod(j)) ' (' num2str(j) ') is downstream of Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') both in Reach ' num2str(id_rch(i)) ' (' num2str(i) ') but its dist_out is larger: ' num2str(dist_out_nod(j)/1000,'%.2f') ' > ' num2str(dist_out_nod(j2)/1000,'%.2f') ' km (gap=' num2str((dist_out_nod(j)-dist_out_nod(j2))/1000,'%.2f') ' km)'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test10a=ok_test10a+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=10.1;
                    list_case_test(nb_case_test,3)=j;
                    list_case_test(nb_case_test,4)=dist_out_nod(j2)-dist_out_nod(j);
                    type_test10a(id_rch_t(i))=type_test10a(id_rch_t(i))+1;
                end
                change_dist_out=abs(dist_out_nod(j2)-dist_out_nod(j));
                if change_dist_out>length_max_node
                    msg_txt=['Dist_out between consecutive Nodes ' num2str(id_nod(j)) ' (' num2str(j) ') and ' num2str(id_nod(j2)) ' (' num2str(j2) ') both in Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is changing a lot: ' num2str(change_dist_out/1000,'%.2f') ' km [' num2str(dist_out_nod(j)/1000,'%.2f') '->' num2str(dist_out_nod(j2)/1000,'%.2f') ']'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test10b=ok_test10b+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=10.2;
                    list_case_test(nb_case_test,3)=j;
                    list_case_test(nb_case_test,4)=dist_out_nod(j2)-dist_out_nod(j);
                    type_test10b(id_rch_t(i))=type_test10b(id_rch_t(i))+1;
                    change_dist_out_maxb=max(change_dist_out_maxb,change_dist_out);
                end
            end
            % We check also the dist_out between the last Node index_rch_node(i,2) of this Reach i, and the first one of its upstream Reaches (if any)
            ok_test10c_local=0; % Check if with one of the upstream Reach the Test 10c is ok
            ok_test10d_local=0; % Check if with one of the upstream Reach the Test 10d is ok
            bufferc={}; % Initialiser le buffer
            bufferd={}; % Initialiser le buffer
            for j=1:n_rch_up_rch(i)
                id1=rch_id_up_rch(i,j); % jth Reach id connected upstream of Reach index i
                index1=find(id_rch==id1,1,'first'); % Its index
                if isempty(index1)
                    continue
                end
                % j1=index_rch_node(i,2); % Last (so upstream) Node index of current Reach i
                % [~,j1]=max(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j1)
                %     j1=index_rch_node(i,1)+j1-1;
                % else
                %     continue
                % end
                % if id_nod_rch(j1)~=id_rch(i)
                %     % The Node j1 is not in the Reach i
                %     continue
                % end
                j1=find_node(i,0,'max');
                if isempty(j1)
                    continue
                end
                % j2=index_rch_node(index1,1); % First (so downstream) Node index of Reach index1 which is upstream of Reach i
                % [~,j2]=min(id_nod(index_rch_node(index1,1):index_rch_node(index1,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j2)
                %     j2=index_rch_node(index1,1)+j2-1;
                % else
                %     continue
                % end
                % if id_nod_rch(j2)~=id_rch(i)
                %     % The Node j2 is not in the Reach i
                %     continue
                % end
                j2=find_node(index1,0,'min');
                if isempty(j2)
                    continue
                end
                % We limit this test for Nodes whose main_side_nod(i)<=opt_warning_10cd (0 for main network, 1 for side network, 2 for secondary outlet)
                if exist('main_side_nod','var')
                    if main_side_nod(j1)>opt_warning_10cd || main_side_nod(j2)>opt_warning_10cd
                        continue
                    end
                end
                if ok_test10c_local==0
                    if dist_out_nod(j2)<dist_out_nod(j1)
                        msg_txt=['Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') in Reach ' num2str(id_rch(i)) '(' num2str(i) ') is downstream of Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') in Reach ' num2str(id_rch(index1)) '(' num2str(index1) ') but its dist_out is larger: ' num2str(dist_out_nod(j1)/1000,'%.2f') ' > ' num2str(dist_out_nod(j2)/1000,'%.2f') ' km (gap=' num2str((dist_out_nod(j1)-dist_out_nod(j2))/1000,'%.2f') ' km)'];
                        bufferc{end+1}=msg_txt;
                        % if opt_disp_details>0
                        %     disp(msg_txt)
                        % end
                        % if opt_wrtlog>1 && opt_wrtlog_details>0
                        %     fprintf(fidLog,'%s\r\n',msg_txt);
                        % end
                        ok_test10c=ok_test10c+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=10.3;
                        list_case_test(nb_case_test,3)=j1;
                        list_case_test(nb_case_test,4)=dist_out_nod(j2)-dist_out_nod(j1);
                        type_test10c(id_rch_t(i))=type_test10c(id_rch_t(i))+1;
                    else
                        % Le problème est résolu grace à une connexion amont (en valeur croissante)
                        ok_test10c_local=1;
                        % On mémorise le noeud amont qui correspond à la continuité du dist_out (en valeur croissante)
                        j2c=j2;
                        % On supprime les anciens problèmes stockés dans ce run, pour ce type d'erreurs et ce noeud j1 du bief i en cours
                        if ~isempty(list_case_test)
                            nb_test10c_local=find(list_case_test(:,1)==i_run & abs(list_case_test(:,2)-10.3)<0.05 & list_case_test(:,3)==j1);
                            if ~isempty(nb_test10c_local)
                                ok_test10c=ok_test10c-length(nb_test10c_local);
                                type_test10c(id_rch_t(i))=type_test10c(id_rch_t(i))-length(nb_test10c_local);
                                nb_case_test=nb_case_test-length(nb_test10c_local);
                                list_case_test(nb_test10c_local,:)=[]; % We remove the previously stored cases
                            end
                        end
                    end
                end
                if ok_test10d_local==0
                    change_dist_out=abs(dist_out_nod(j2)-dist_out_nod(j1));
                    if change_dist_out>length_max_node
                        msg_txt=['Dist_out between last Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and first Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of an upstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is changing a lot: ' num2str(change_dist_out/1000,'%.2f') ' km [' num2str(dist_out_nod(j1)/1000,'%.2f') '->' num2str(dist_out_nod(j2)/1000,'%.2f') ']'];
                        bufferd{end+1}=msg_txt;
                        % if opt_disp_details>0
                        %     disp(msg_txt)
                        % end
                        % if opt_wrtlog>1 && opt_wrtlog_details>0
                        %     fprintf(fidLog,'%s\r\n',msg_txt);
                        % end
                        ok_test10d=ok_test10d+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=10.4;
                        list_case_test(nb_case_test,3)=j1;
                        list_case_test(nb_case_test,4)=dist_out_nod(j2)-dist_out_nod(j1);
                        type_test10d(id_rch_t(i))=type_test10d(id_rch_t(i))+1;
                        change_dist_out_maxd=max(change_dist_out_maxd,change_dist_out);
                    else
                        % Le problème est résolu grace à une connexion amont (en change_dist_out)
                        ok_test10d_local=1;
                        % On mémorise le noeud amont qui correspond à la continuité du dist_out (en change_dist_out)
                        j2d=j2;
                        change_dist_outd=change_dist_out;
                        % On supprime les anciens problèmes stockés dans ce run, pour ce type d'erreurs et ce noeud j1 du bief i en cours
                        if ~isempty(list_case_test)
                            nb_test10d_local=find(list_case_test(:,1)==i_run & abs(list_case_test(:,2)-10.4)<0.05 & list_case_test(:,3)==j1);
                            if ~isempty(nb_test10d_local)
                                ok_test10d=ok_test10d-length(nb_test10d_local);
                                type_test10d(id_rch_t(i))=type_test10d(id_rch_t(i))-length(nb_test10d_local);
                                nb_case_test=nb_case_test-length(nb_test10d_local);
                                list_case_test(nb_test10d_local,:)=[]; % We remove the previously stored cases
                            end
                        end
                    end
                end
            end
            if ok_test10c_local==1
                if ~isempty(bufferc)
                    % On n'écrit que c'est ok que si le buffer n'est pas vide, donc que c'était pas la seule connexion
                    msg_txt=['Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') is downstream of Node ' num2str(id_nod(j2c)) ' (' num2str(j2c) ') and its dist_out is correctly smaller: ' num2str(dist_out_nod(j1)/1000,'%.2f') ' > ' num2str(dist_out_nod(j2c)/1000,'%.2f') ' km (gap=' num2str((dist_out_nod(j1)-dist_out_nod(j2c))/1000,'%.2f') ' km)'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            else % Écrire le contenu du buffer dans le fichier log
                for j=1:length(bufferc)
                    msg_txt=bufferc{j};
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
            if ok_test10d_local==1
                if ~isempty(bufferd)
                    % On n'écrit que c'est ok que si le buffer n'est pas vide, donc que c'était pas la seule connexion
                    msg_txt=['Dist_out between last Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and first Node ' num2str(id_nod(j2d)) ' (' num2str(j2d) ') of an upstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is correctly changing within the limit: ' num2str(change_dist_outd/1000,'%.2f') ' km [' num2str(dist_out_nod(j1)/1000,'%.2f') '->' num2str(dist_out_nod(j2d)/1000,'%.2f') ']'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            else % Écrire le contenu du buffer dans le fichier log
                for j=1:length(bufferd)
                    msg_txt=bufferd{j};
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
            % We check also the dist_out between the first Node index_rch_node(i,1) of this Reach i, and the last one of its downstream Reaches (if any)
            ok_test10c_local=0; % Check if with one of the downstream Reach the Test 10c is ok
            ok_test10d_local=0; % Check if with one of the downstream Reach the Test 10d is ok
            bufferc={}; % Initialiser le buffer
            bufferd={}; % Initialiser le buffer
            for j=1:n_rch_dn_rch(i)
                id1=rch_id_dn_rch(i,j); % jth Reach id connected downstream of Reach index i
                index1=find(id_rch==id1,1,'first'); % Its index
                if isempty(index1)
                    continue
                end
                % j1=index_rch_node(i,1); % First (so downstream) Node index of current Reach i
                % [~,j1]=min(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j1)
                %     j1=index_rch_node(i,1)+j1-1;
                % else
                %     continue
                % end
                j1=find_node(i,0,'min');
                if isempty(j1)
                    continue
                end
                % j2=index_rch_node(index1,2); % Last (so upstream) Node index of Reach index1 which is downstream of Reach i
                % [~,j2]=max(id_nod(index_rch_node(index1,1):index_rch_node(index1,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j2)
                %     j2=index_rch_node(index1,1)+j2-1;
                % else
                %     continue
                % end
                j2=find_node(index1,0,'max');
                if isempty(j2)
                    continue
                end
                % We limit this test for Nodes whose main_side_nod(i)<=opt_warning_10cd (0 for main network, 1 for side network, 2 for secondary outlet)
                if exist('main_side_nod','var')
                    if main_side_nod(j1)>opt_warning_10cd || main_side_nod(j2)>opt_warning_10cd
                        continue
                    end
                end
                if ok_test10c_local==0
                    if dist_out_nod(j2)>dist_out_nod(j1)
                        msg_txt=['Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') is upstream of Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') but its dist_out is smaller: ' num2str(dist_out_nod(j1)/1000,'%.2f') ' < ' num2str(dist_out_nod(j2)/1000,'%.2f') ' km (gap=' num2str((dist_out_nod(j1)-dist_out_nod(j2))/1000,'%.2f') ' km)'];
                        bufferc{end+1}=msg_txt;
                        % if opt_disp_details>0
                        %     disp(msg_txt)
                        % end
                        % if opt_wrtlog>1 && opt_wrtlog_details>0
                        %     fprintf(fidLog,'%s\r\n',msg_txt);
                        % end
                        ok_test10c=ok_test10c+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=10.3;
                        list_case_test(nb_case_test,3)=j1;
                        list_case_test(nb_case_test,4)=dist_out_nod(j2)-dist_out_nod(j1);
                        type_test10c(id_rch_t(i))=type_test10c(id_rch_t(i))+1;
                    else
                        % Le problème est résolu grace à une connexion aval (en valeur décroissante)
                        ok_test10c_local=1;
                        % On mémorise le noeud aval qui correspond à la continuité du dist_out (en valeur décroissante)
                        j2c=j2;
                        % On supprime les anciens problèmes stockés dans ce run, pour ce type d'erreurs et ce noeud j1 du bief i en cours
                        if ~isempty(list_case_test)
                            nb_test10c_local=find(list_case_test(:,1)==i_run & abs(list_case_test(:,2)-10.3)<0.05 & list_case_test(:,3)==j1);
                            if ~isempty(nb_test10c_local)
                                ok_test10c=ok_test10c-length(nb_test10c_local);
                                type_test10c(id_rch_t(i))=type_test10c(id_rch_t(i))-length(nb_test10c_local);
                                nb_case_test=nb_case_test-length(nb_test10c_local);
                                list_case_test(nb_test10c_local,:)=[]; % We remove the previously stored cases
                            end
                        end
                    end
                end
                if ok_test10d_local==0
                    change_dist_out=abs(dist_out_nod(j2)-dist_out_nod(j1));
                    if change_dist_out>length_max_node
                        msg_txt=['Dist_out between first Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and last Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of a downstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is changing a lot: ' num2str(change_dist_out/1000,'%.2f') ' km [' num2str(dist_out_nod(j1)/1000,'%.2f') '->' num2str(dist_out_nod(j2)/1000,'%.2f') ']'];
                        bufferd{end+1}=msg_txt;
                        % if opt_disp_details>0
                        %     disp(msg_txt)
                        % end
                        % if opt_wrtlog>1 && opt_wrtlog_details>0
                        %     fprintf(fidLog,'%s\r\n',msg_txt);
                        % end
                        ok_test10d=ok_test10d+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=10.4;
                        list_case_test(nb_case_test,3)=j1;
                        list_case_test(nb_case_test,4)=dist_out_nod(j2)-dist_out_nod(j1);
                        type_test10d(id_rch_t(i))=type_test10d(id_rch_t(i))+1;
                        change_dist_out_maxd=max(change_dist_out_maxd,change_dist_out);
                    else
                        % Le problème est résolu grace à une connexion aval (en change_dist_out)
                        ok_test10d_local=1;
                        % On mémorise le noeud aval qui correspond à la continuité du dist_out (en change_dist_out)
                        j2d=j2;
                        change_dist_outd=change_dist_out;
                        % On supprime les anciens problèmes stockés dans ce run, pour ce type d'erreurs et ce noeud j1 du bief i en cours
                        if ~isempty(list_case_test)
                            nb_test10d_local=find(list_case_test(:,1)==i_run & abs(list_case_test(:,2)-10.4)<0.05 & list_case_test(:,3)==j1);
                            if ~isempty(nb_test10d_local)
                                ok_test10d=ok_test10d-length(nb_test10d_local);
                                type_test10d(id_rch_t(i))=type_test10d(id_rch_t(i))-length(nb_test10d_local);
                                nb_case_test=nb_case_test-length(nb_test10d_local);
                                list_case_test(nb_test10d_local,:)=[]; % We remove the previously stored cases
                            end
                        end
                    end
                end
            end
            if ok_test10c_local==1
                if ~isempty(bufferc)
                    % On n'écrit que c'est ok que si le buffer n'est pas vide, donc que c'était pas la seule connexion
                    msg_txt=['Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') is upstream of Node ' num2str(id_nod(j2c)) ' (' num2str(j2c) ') and its dist_out is correctly larger: ' num2str(dist_out_nod(j1)/1000,'%.2f') ' > ' num2str(dist_out_nod(j2c)/1000,'%.2f') ' km (gap=' num2str((dist_out_nod(j1)-dist_out_nod(j2c))/1000,'%.2f') ' km)'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            else % Écrire le contenu du buffer dans le fichier log
                for j=1:length(bufferc)
                    msg_txt=bufferc{j};
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
            if ok_test10d_local==1
                if ~isempty(bufferd)
                    % On n'écrit que c'est ok que si le buffer n'est pas vide, donc que c'était pas la seule connexion
                    msg_txt=['Dist_out between first Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and last Node ' num2str(id_nod(j2d)) ' (' num2str(j2d) ') of a downstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is correctly changing within the limit: ' num2str(change_dist_outd/1000,'%.2f') ' km [' num2str(dist_out_nod(j1)/1000,'%.2f') '->' num2str(dist_out_nod(j2d)/1000,'%.2f') ']'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            else % Écrire le contenu du buffer dans le fichier log
                for j=1:length(bufferd)
                    msg_txt=bufferd{j};
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
            if i>step_value
                time_duration=toc(lapstime);
                disp([num2str(step_percent*100) ' % ... (' num2str(time_duration) ' s)'])
                step_percent=step_percent+0.1;
                step_value=floor(nb_rch*step_percent);
                lapstime=tic;
            end
        end
        if ok_test8a==0
            msg_txt=['+> Test 8a passed (indexes of first and last Nodes in a Reach seem correct, ie first < last)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 8a failed (indexes of first and last Nodes in a Reach are wrong) ' num2str(ok_test8a) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test8a/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test8a(i)>0
                    msg_txt=['-> Test 8a per type ' num2str(i) ' = ' num2str((type_test8a(i)/ok_test8a)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test8b==0
            msg_txt=['+> Test 8b passed (number of Nodes in a Reach is correct)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 8b failed (number of Nodes in a Reach is not correct) ' num2str(ok_test8b) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test8b/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test8b(i)>0
                    msg_txt=['-> Test 8b per type ' num2str(i) ' = ' num2str((type_test8b(i)/ok_test8b)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test9a==0
            msg_txt=['+> Test 9a passed (allocation of Nodes in the correct Reach is ok)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 9a failed (allocation of Nodes in the correct Reach is wrong) ' num2str(ok_test9a) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test9a/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test9a(i)>0
                    msg_txt=['-> Test 9a per type ' num2str(i) ' = ' num2str((type_test9a(i)/ok_test9a)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test9b==0
            msg_txt=['+> Test 9b passed (Nodes indexes are continuous inside their Reach)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 9b failed (Nodes indexes are not continuous inside their Reach) ' num2str(ok_test9b) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test9b/nb_rch_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test9b(i)>0
                    msg_txt=['-> Test 9b per type ' num2str(i) ' = ' num2str((type_test9b(i)/ok_test9b)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test9c==0
            msg_txt=['+> Test 9c passed (allocation of Centreline Points in the correct Reach is ok)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 9c failed (allocation of Centreline Points in the correct Reach is wrong) ' num2str(ok_test9c) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test9c/nb_rch_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test9c(i)>0
                    msg_txt=['-> Test 9c per type ' num2str(i) ' = ' num2str((type_test9c(i)/ok_test9c)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test9d==0
            msg_txt=['+> Test 9d passed (allocation of Centreline Points in the correct Node is ok)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 9d failed (allocation of Centreline Points in the correct Node is wrong) ' num2str(ok_test9d) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test9d/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test9d(i)>0
                    msg_txt=['-> Test 9d per type ' num2str(i) ' = ' num2str((type_test9d(i)/ok_test9d)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test10a==0
            msg_txt=['+> Test 10a passed (dist_out is increasing upstream for Nodes)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 10a failed (dist_out is not increasing upstream for Nodes) ' num2str(ok_test10a) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test10a/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test10a(i)>0
                    msg_txt=['-> Test 10a per type ' num2str(i) ' = ' num2str((type_test10a(i)/ok_test10a)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test10b==0
            msg_txt=['+> Test 10b passed (dist_out for Nodes is increasing reasonably, less than ' num2str(length_max_node,'%.0f') ' m'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 10b failed (dist_out for Nodes is increasing maybe too much) ' num2str(ok_test10b) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test10b/nb_nod_verified)*100,'%.3f') '%). More than ' num2str(length_max_node,'%.0f') ' m, up to ' num2str(change_dist_out_maxb/1000,'%.2f') ' km'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test10b(i)>0
                    msg_txt=['-> Test 10b per type ' num2str(i) ' = ' num2str((type_test10b(i)/ok_test10b)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test10c==0
            msg_txt=['+> Test 10c passed (dist_out is increasing upstream for boundary Nodes of connected Reaches)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 10c failed (dist_out is not increasing upstream for boundary Nodes of connected Reaches) ' num2str(ok_test10c) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test10c/nb_rch_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test10c(i)>0
                    msg_txt=['-> Test 10c per type ' num2str(i) ' = ' num2str((type_test10c(i)/ok_test10c)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test10d==0
            msg_txt=['+> Test 10d passed (dist_out for boundary Nodes of connected Reaches is increasing reasonably, less than ' num2str(length_max_node,'%.0f') ' m)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 10d failed (dist_out for boundary Nodes of connected Reaches is increasing maybe too much) ' num2str(ok_test10d) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test10d/nb_rch_verified)*100,'%.3f') '%). More than ' num2str(length_max_node,'%.0f') ' m, up to ' num2str(change_dist_out_maxd/1000,'%.2f') ' km'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test10d(i)>0
                    msg_txt=['-> Test 10d per type ' num2str(i) ' = ' num2str((type_test10d(i)/ok_test10d)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
    else
        msg_txt=['-> Tests 8a, 8b, 9a, 9b, 10a, 10b, 10c, 10d not run since index_rch_node has not been calculated (boost mode not activated)!'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    % ===========================TEST 11 ======================================
    % When 2 Reaches are connected the geolocation of the corresponding closest Nodes should correspond to about 200m distance.
    % Otherwise the Nodes are probably in the wrong order and should be inversed (or the Reach flow direction is wrong)
    % But, when it is a tributary the Reach can be connected somewhere inside, to an intermediate Node (POM 23/09/24)
    if opt_sword_comp~=0
        ok_test11a=0; % Nodes distance suspiciously too large upstream
        ok_test11b=0; % Nodes distance suspiciously too large downstream
        ok_test11c=0; % Nodes distance ok but with tributary arriving as upstream
        ok_test11d=0; % Nodes distance ok but with tributary leaving as downstream
        type_test11a(1:6)=zeros(1,6);
        type_test11b(1:6)=zeros(1,6);
        type_test11c(1:6)=zeros(1,6);
        type_test11d(1:6)=zeros(1,6);
        corrected_11=0;
        length_max_node=400;
        %length_max_node=600; % On passe à 600 m pour déclancher moins de warnings
        length_max_node_in=300; % Distance pour les connexions internes (tributary)
        change_dist_loc=0;
        change_dist_loc_up_max=0;
        change_dist_loc_dn_max=0;
        change_dist_loc_up_new_max=0;
        change_dist_loc_dn_new_max=0;
        for i=1:nb_rch
            if opt_filter_validity>0 && ~ismember(i,ref_rch)
                continue
            end
            if ~id_rch_filter(i)
                continue
            end
            pb_up=0; % Problem of (large) distance from geolocation at the upstream Node of the Reach i
            pb_dn=0; % Same at downstream
            ok_up=0; % Good (small) distance from geolocation at the upstream Node of the Reach i
            ok_in=0; % Same at intermediate location (tributary)
            ok_dn=0; % Same at downstream
            % We check also the distance calculated from the geoloc the last (upstream) Node index_rch_node(i,2) of this Reach i, and the first (downstream) one of its upstream Reaches (if any)
            for j=1:n_rch_up_rch(i)
                id1=rch_id_up_rch(i,j); % jth Reach id connected upstream of Reach index i
                index1=find(id_rch==id1,1,'first'); % Its index
                if isempty(index1)
                    continue
                end
                % j1=index_rch_node(i,2); % Last (so upstream) Node index of current Reach i
                % [~,j1]=max(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j1)
                %     j1=index_rch_node(i,1)+j1-1;
                % else
                %     continue
                % end
                j1=find_node(i,0,'max');
                if isempty(j1)
                    continue
                end
                % j2=index_rch_node(index1,1); % First (so downstream) Node index of Reach index1 which is upstream of Reach i
                % [~,j2]=min(id_nod(index_rch_node(index1,1):index_rch_node(index1,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j2)
                %     j2=index_rch_node(index1,1)+j2-1;
                % else
                %     continue
                % end
                j2=find_node(index1,0,'min');
                if isempty(j2)
                    continue
                end
                change_dist_loc=lldistkm([y_nod(j1) x_nod(j1)],[y_nod(j2) x_nod(j2)])*1000;
                if change_dist_loc>length_max_node
                    msg_txt=['Geoloc Dist_loc between last (upstream) Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and first (downstream) Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of an upstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is suspiciously too large: ' num2str(change_dist_loc/1000,'%.2f') ' km'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    % We test if the Reach index1 arrives as a tributary somewhere inside Reach i (not at boundaries)
                    case_tributary=false;
                    % [~,j3]=min(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                    % if ~isempty(j3)
                    %     j3=index_rch_node(i,1)+j3-1;
                    % else
                    %     continue
                    % end
                    j3=find_node(i,0,'min');
                    if isempty(j3)
                        continue
                    end
                    for k1=index_rch_node(i,1):index_rch_node(i,2)
                        if id_nod_rch(k1)~=id_rch(i)
                            % The Node k1 is not in the Reach i
                            continue
                        end
                        if k1==j1 || k1==j3
                            % All Nodes are checked except the last upstream one already checked and first downstream one we do not allow
                            continue
                        end
                        change_dist_loc_in=lldistkm([y_nod(k1) x_nod(k1)],[y_nod(j2) x_nod(j2)])*1000;
                        if change_dist_loc_in<=length_max_node_in
                            msg_txt=['But ... Geoloc Dist_loc between intermediate Node ' num2str(id_nod(k1)) ' (' num2str(k1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and first (downstream) Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of an upstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is ok: ' num2str(change_dist_loc_in,'%.2f') ' m'];
                            if opt_disp_details>0
                                disp(msg_txt)
                            end
                            if opt_wrtlog>1 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            case_tributary=true;
                            %break
                        end
                    end
                    if case_tributary
                        change_dist_loc_up_max=max(change_dist_loc_up_max,change_dist_loc_in);
                        ok_test11c=ok_test11c+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=11.3;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=change_dist_loc_up_max;
                        type_test11c(id_rch_t(i))=type_test11c(id_rch_t(i))+1;
                        ok_in=ok_in+1;
                    else
                        change_dist_loc_up_max=max(change_dist_loc_up_max,change_dist_loc);
                        ok_test11a=ok_test11a+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=11.1;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=change_dist_loc_up_max;
                        type_test11a(id_rch_t(i))=type_test11a(id_rch_t(i))+1;
                        pb_up=pb_up+1;
                    end
                else
                    change_dist_loc_up_max=max(change_dist_loc_up_max,change_dist_loc);
                    ok_up=ok_up+1;
                end
            end
            % We check also the distance calculated from the geoloc between the first (downstream) Node index_rch_node(i,1) of this Reach i, and the last (upstream) one of its downstream Reaches (if any)
            for j=1:n_rch_dn_rch(i)
                id1=rch_id_dn_rch(i,j); % jth Reach id connected downstream of Reach index i
                index1=find(id_rch==id1,1,'first'); % Its index
                if isempty(index1)
                    continue
                end
                % j1=index_rch_node(i,1); % First (so downstream) Node index of current Reach i
                % [~,j1]=min(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j1)
                %     j1=index_rch_node(i,1)+j1-1;
                % else
                %     continue
                % end
                j1=find_node(i,0,'min');
                if isempty(j1)
                    continue
                end
                % j2=index_rch_node(index1,2); % Last (so upstream) Node index of Reach index1 which is downstream of Reach i
                % [~,j2]=max(id_nod(index_rch_node(index1,1):index_rch_node(index1,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                % if ~isempty(j2)
                %     j2=index_rch_node(index1,1)+j2-1;
                % else
                %     continue
                % end
                j2=find_node(index1,0,'max');
                if isempty(j2)
                    continue
                end
                change_dist_loc=lldistkm([y_nod(j1) x_nod(j1)],[y_nod(j2) x_nod(j2)])*1000;
                if change_dist_loc>length_max_node
                    msg_txt=['Geoloc Dist_loc between first (downstream) Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and last (upstream) Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of a downstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is suspiciously too large: ' num2str(change_dist_loc/1000,'%.2f') ' km'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    % We test if the Reach index1 leaves as a tributary somewhere inside Reach i
                    case_tributary=false;
                    % [~,j3]=max(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                    % if ~isempty(j3)
                    %     j3=index_rch_node(i,1)+j3-1;
                    % else
                    %     continue
                    % end
                    j3=find_node(i,0,'max');
                    if isempty(j3)
                        continue
                    end
                    for k1=index_rch_node(i,1):index_rch_node(i,2)
                        if id_nod_rch(k1)~=id_rch(i)
                            % The Node k1 is not in the Reach i
                            continue
                        end
                        if k1==j1 || k1==j3
                            % All Nodes are checked except the first downstream one already checked and last upstream one we do not allow
                            continue
                        end
                        change_dist_loc_in=lldistkm([y_nod(k1) x_nod(k1)],[y_nod(j2) x_nod(j2)])*1000;
                        if change_dist_loc_in<=length_max_node_in
                            msg_txt=['But ... Geoloc Dist_loc between intermediate Node ' num2str(id_nod(k1)) ' (' num2str(k1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and last (upstream) Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of a downstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is ok: ' num2str(change_dist_loc_in,'%.2f') ' m'];
                            if opt_disp_details>0
                                disp(msg_txt)
                            end
                            if opt_wrtlog>1 && opt_wrtlog_details>0
                                fprintf(fidLog,'%s\r\n',msg_txt);
                            end
                            case_tributary=true;
                            %break
                        end
                    end
                    if case_tributary
                        change_dist_loc_dn_max=max(change_dist_loc_dn_max,change_dist_loc_in);
                        ok_test11d=ok_test11d+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=11.4;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=change_dist_loc_dn_max;
                        type_test11d(id_rch_t(i))=type_test11d(id_rch_t(i))+1;
                        ok_in=ok_in+1;
                    else
                        change_dist_loc_dn_max=max(change_dist_loc_dn_max,change_dist_loc);
                        ok_test11b=ok_test11b+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=11.2;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=change_dist_loc_dn_max;
                        type_test11b(id_rch_t(i))=type_test11b(id_rch_t(i))+1;
                        pb_dn=pb_dn+1;
                    end
                else
                    change_dist_loc_dn_max=max(change_dist_loc_dn_max,change_dist_loc);
                    ok_dn=ok_dn+1;
                end
            end
            if opt_sword_corr_auto>0 && i_run>=i_run_min
                if pb_up>0 || pb_dn>0
                    % Probably Reach i has inversed Nodes
                    % We check if by reversing Nodes if it gets better (new distance becomes about 200m)
                    pb_up_new=0;
                    pb_dn_new=0;
                    ok_up_new=0;
                    ok_dn_new=0;
                    % We check also the distance calculated from the geoloc the last (upstream) Node index_rch_node(i,2) of this Reach i, and the last (upstream) one of its downstream Reaches (if any)
                    for j=1:n_rch_dn_rch(i)
                        id1=rch_id_dn_rch(i,j); % jth Reach id connected upstream of Reach index i
                        index1=find(id_rch==id1,1,'first'); % Its index
                        if isempty(index1)
                            continue
                        end
                        % j1=index_rch_node(i,2); % Last (so upstream) Node index of current Reach i
                        % [~,j1]=max(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                        % if ~isempty(j1)
                        %     j1=index_rch_node(i,1)+j1-1;
                        % else
                        %     continue
                        % end
                        j1=find_node(i,0,'max');
                        if isempty(j1)
                            continue
                        end
                        % j2=index_rch_node(index1,2); % Last (so upstream) Node index of Reach index1 which is downstream of Reach i
                        % [~,j2]=max(id_nod(index_rch_node(index1,1):index_rch_node(index1,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                        % if ~isempty(j2)
                        %     j2=index_rch_node(index1,1)+j2-1;
                        % else
                        %     continue
                        % end
                        j2=find_node(index1,0,'max');
                        if isempty(j2)
                            continue
                        end
                        change_dist_loc=lldistkm([y_nod(j1) x_nod(j1)],[y_nod(j2) x_nod(j2)])*1000;
                        change_dist_loc_dn_new_max=max(change_dist_loc_dn_new_max,change_dist_loc);
                        if change_dist_loc<length_max_node
                            % disp(['Dist_loc between last (upstream) Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and first (downstream) Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of an upstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is suspiciously too large: ' num2str(change_dist_loc/1000,'%.2f') ' km'])
                            % ok_test11a=ok_test11a+1;
                            ok_dn_new=ok_dn_new+1;
                        else
                            pb_dn_new=pb_dn_new+1;
                        end
                    end
                    % We check also the distance calculated from the geoloc between the first (downstream) Node index_rch_node(i,1) of this Reach i, and the first (downstream) one of its upstream Reaches (if any)
                    for j=1:n_rch_up_rch(i)
                        id1=rch_id_up_rch(i,j); % jth Reach id connected downstream of Reach index i
                        index1=find(id_rch==id1,1,'first'); % Its index
                        if isempty(index1)
                            continue
                        end
                        % j1=index_rch_node(i,1); % First (so downstream) Node index of current Reach i
                        % [~,j1]=min(id_nod(index_rch_node(i,1):index_rch_node(i,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                        % if ~isempty(j1)
                        %     j1=index_rch_node(i,1)+j1-1;
                        % else
                        %     continue
                        % end
                        j1=find_node(i,0,'min');
                        if isempty(j1)
                            continue
                        end
                        % j2=index_rch_node(index1,1); % First (so downstream) Node index of Reach index1 which is upstream of Reach i
                        % [~,j2]=min(id_nod(index_rch_node(index1,1):index_rch_node(index1,2))); % On teste sur les ID et non plus sur les index (POM 03/04/25)
                        % if ~isempty(j2)
                        %     j2=index_rch_node(index1,1)+j2-1;
                        % else
                        %     continue
                        % end
                        j2=find_node(index1,0,'min');
                        if isempty(j2)
                            continue
                        end
                        change_dist_loc=lldistkm([y_nod(j1) x_nod(j1)],[y_nod(j2) x_nod(j2)])*1000;
                        change_dist_loc_up_new_max=max(change_dist_loc_up_new_max,change_dist_loc);
                        if change_dist_loc<length_max_node
                            % disp(['Dist_loc between first (downstream) Node ' num2str(id_nod(j1)) ' (' num2str(j1) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') and last (upstream) Node ' num2str(id_nod(j2)) ' (' num2str(j2) ') of a downstream Reach ' num2str(id_rch(index1)) ' (' num2str(index1) ') is suspiciously too large: ' num2str(change_dist_loc/1000,'%.2f') ' km'])
                            % ok_test11b=ok_test11b+1;
                            ok_up_new=ok_up_new+1;
                        else
                            pb_up_new=pb_up_new+1;
                        end
                    end
                    if opt_reverse_11==0
                        ok_reverse=0;
                    elseif opt_reverse_11==1
                        ok_reverse=(ok_up_new+ok_dn_new>=ok_up+ok_dn);
                    elseif opt_reverse_11==2
                        ok_reverse=(ok_up_new+ok_dn_new>ok_up+ok_dn);
                    end
                    patch_comment=['Pb=' num2str(pb_up+pb_dn) ', Ok=' num2str(ok_up+ok_dn) ' -> Pb=' num2str(pb_up_new+pb_dn_new) ', Ok=' num2str(ok_up_new+ok_dn_new)];
                    if ok_reverse
                        % Ok we ask for a correction (it is better after inversion of Nodes, than before)
                        disp(['Automatic correction: correct_sword_node_order(' num2str(id_rch(i)) ',' patch_comment ') : ' patch_comment])
                        correct_sword_node_order(id_rch(i),patch_comment);
                        corrected_11=corrected_11+1;
                    else
                        if ok_correct_sword_other==1
                            disp(['No correction for ' num2str(id_rch(i)) ', since ' patch_comment ' and opt_reverse_11 mode=' num2str(opt_reverse_11) '. But indication in the json file with correct_sword_other'])
                            correct_sword_other(id_rch(i),patch_comment);
                        end
                    end
                end
            end
        end
        if ok_test11a==0
            msg_txt=['+> Test 11a passed (distance from geolocation of first and last Nodes of connected Reaches upstream seem correct, less than ' num2str(length_max_node,'%.0f') ' m)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 11a failed (distance from geolocation of first and last Nodes of connected Reaches upstream seem incorrect) ' num2str(ok_test11a) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test11a/nb_rch_verified)*100,'%.3f') '%). More than ' num2str(length_max_node,'%.0f') ' m, up to ' num2str(change_dist_loc_up_max/1000,'%.2f') ' km'];
            disp(msg_txt)
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            for i=1:6
                if type_test11a(i)>0
                    msg_txt=['-> Test 11a per type ' num2str(i) ' = ' num2str((type_test11a(i)/ok_test11a)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test11c>0
            msg_txt=['+> Test 11c passed thanks to upstream tributaries arriving inside a Reach, ' num2str(ok_test11c) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test11c/nb_rch_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            for i=1:6
                if type_test11c(i)>0
                    msg_txt=['-> Test 11c per type ' num2str(i) ' = ' num2str((type_test11c(i)/ok_test11c)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test11b==0
            msg_txt=['+> Test 11b passed (distance from geolocation of first and last Nodes of connected Reaches downstream seem correct), less than ' num2str(length_max_node,'%.0f') ' m'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 11b failed (distance from geolocation of first and last Nodes of connected Reaches downstream seem incorrect) ' num2str(ok_test11b) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test11b/nb_rch_verified)*100,'%.3f') '%). More than ' num2str(length_max_node,'%.0f') ' m, up to ' num2str(change_dist_loc_dn_max/1000,'%.2f') ' km'];
            disp(msg_txt)
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            for i=1:6
                if type_test11b(i)>0
                    msg_txt=['-> Test 11b per type ' num2str(i) ' = ' num2str((type_test11b(i)/ok_test11b)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test11d>0
            msg_txt=['+> Test 11d passed thanks to downstream tributaries leaving from inside a Reach, ' num2str(ok_test11d) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test11d/nb_rch_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            for i=1:6
                if type_test11d(i)>0
                    msg_txt=['-> Test 11d per type ' num2str(i) ' = ' num2str((type_test11d(i)/ok_test11d)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if opt_sword_corr_auto>0
            if corrected_11>0
                msg_txt=['-> Test 11 : ' num2str(corrected_11) ' automatic corrections suggested in the csv and json files'];
                disp(msg_txt)
                if opt_pause>0
                    disp('Press a key to continue ...')
                    pause
                end
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    else
        corrected_11=0;
        msg_txt=['-> Tests 11a, 11b not run since index_rch_node has not been calculated (boost mode not activated)!'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    % ===========================TEST 12 ======================================
    % Nodes index should be also from downstream to upstream, same as ID (Test 12a). Euh pas vrai en fait ... (POM 03/04/25)
    % Reaches ID's should be correct (11 characters) (Test 12b)
    % Nodes ID's should be correct (11 & 14 characters, same Reach, same Type, etc) (Test 12c) -> Correction available
    if opt_sword_comp~=0
        ok_test12a=0;
        ok_test12b=0;
        ok_test12c=0;
        type_test12a(1:6)=zeros(1,6);
        type_test12b(1:6)=zeros(1,6);
        type_test12c(1:6)=zeros(1,6);
        corrected_12c=0;
        for i=1:nb_rch
            if opt_filter_validity>0 && ~ismember(i,ref_rch)
                continue
            end
            if ~id_rch_filter(i)
                continue
            end
            if opt_warning_12a>0
                for j=index_rch_node(i,1):(index_rch_node(i,2)-1)
                    if id_nod_rch(j)~=id_rch(i) || id_nod_rch(j+1)~=id_rch(i)
                        % The Node j or j+1 is not in the Reach i
                        continue
                    end
                    if id_nod(j+1)<id_nod(j)
                        msg_txt=['Warning: Node ' num2str(id_nod(j)) ' (' num2str(j) ') is stored before of Node ' num2str(id_nod(j+1)) ' (' num2str(j+1) ') but its ID is larger: ' num2str(id_nod(j),'%d') ' > ' num2str(id_nod(j+1),'%d')];
                        if opt_disp_details>0
                            disp(msg_txt)
                        end
                        if opt_wrtlog>1 && opt_wrtlog_details>0
                            fprintf(fidLog,'%s\r\n',msg_txt);
                        end
                        ok_test12a=ok_test12a+1;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=12.1;
                        list_case_test(nb_case_test,3)=i;
                        list_case_test(nb_case_test,4)=id_nod(j+1)-id_nod(j);
                        type_test12a(id_rch_t(i))=type_test12a(id_rch_t(i))+1;
                        if j==index_rch_node(i,2)-1
                            % En bordure on ajoute 1 de plus pour avoir 100%
                            ok_test12a=ok_test12a+1;
                            type_test12a(id_rch_t(i))=type_test12a(id_rch_t(i))+1;
                        end
                    end
                end
            end
            id_rch_s=num2str(id_rch(i));
            % We also test that the last one is 1, 3, 4, 5 or 6
            if length(id_rch_s)~=11 || ~ismember(id_rch_s(11:11),['1' '3' '4' '5' '6'])
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') ID does not have 11 valid characters'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test12b=ok_test12b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=12.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=length(id_rch_s);
                type_test12b(id_rch_t(i))=type_test12b(id_rch_t(i))+1;
            end
            for j=index_rch_node(i,1):index_rch_node(i,2)
                if id_nod_rch(j)~=id_rch(i)
                    % The Node j is not in the Reach i
                    continue
                end
                id_nod_s=num2str(id_nod(j));
                if length(id_nod_s)~=14 || ~isequal(id_nod_s(1:10),id_rch_s(1:10)) || ~isequal(id_nod_s(14:14),id_rch_s(11:11))
                    msg_txt=['Node ' num2str(id_nod(j)) ' (' num2str(j) ') of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') ID does not have coherent characters'];
                    if opt_disp_details>0
                        disp(msg_txt)
                    end
                    if opt_wrtlog>1 && opt_wrtlog_details>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                    ok_test12c=ok_test12c+1;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=12.3;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=length(id_nod_s);
                    type_test12c(id_rch_t(i))=type_test12c(id_rch_t(i))+1;
                    if opt_sword_corr_auto>0 && i_run>=i_run_min
                        if length(id_nod_s)==15 && isequal(id_nod_s(1:10),id_rch_s(1:10)) && isequal(id_nod_s(15:15),id_rch_s(11:11)) && isequal(id_nod_s(11:11),'0')
                            new_attribute_value=str2num([id_nod_s(1:10) id_nod_s(12:15)]);
                        else
                            new_attribute_value=[];
                        end
                        if ~isempty(new_attribute_value)
                            patch_comment=['Old Node Id=' num2str(id_nod(j))];
                            disp(['Automatic correction: correct_sword_node_attribute(' num2str(id_nod(j)) ',7,' num2str(new_attribute_value) ') : ' patch_comment])
                            correct_sword_node_attribute(id_nod(j),7,new_attribute_value,patch_comment);
                            corrected_12c=corrected_12c+1;
                        end
                    end
                end
            end
        end
        if ok_test12a==0
            if opt_warning_12a>0
                msg_txt=['+> Test 12a passed (Nodes ID and Nodes order are coherent)'];
            else
                msg_txt=['+> Test 12a passed (Since not activated - Nodes ID and Nodes order are coherent)'];
            end
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 12a failed (Nodes ID and Nodes order are not coherent) ' num2str(ok_test12a) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test12a/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test12a(i)>0
                    msg_txt=['-> Test 12a per type ' num2str(i) ' = ' num2str((type_test12a(i)/ok_test12a)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test12b==0
            msg_txt=['+> Test 12b passed (Reaches ID seem correct with 11 characters)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 12b failed (Reaches ID seem incorrect without 11 characters) ' num2str(ok_test12b) ' times out of ' num2str(nb_rch_verified) ' Nodes (' num2str((ok_test12b/nb_rch_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test12b(i)>0
                    msg_txt=['-> Test 12b per type ' num2str(i) ' = ' num2str((type_test12b(i)/ok_test12b)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
        if ok_test12c==0
            msg_txt=['+> Test 12c passed (Nodes ID seem correct with 14 characters)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        else
            msg_txt=['-> Test 12c failed (Nodes ID seem incorrect without 14 coherent characters) ' num2str(ok_test12c) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test12c/nb_nod_verified)*100,'%.3f') '%)'];
            disp(msg_txt)
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            for i=1:6
                if type_test12c(i)>0
                    msg_txt=['-> Test 12c per type ' num2str(i) ' = ' num2str((type_test12c(i)/ok_test12c)*100,'%.3f') '%'];
                    disp(msg_txt)
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
            if opt_sword_corr_auto>0
                if corrected_12c>0
                    msg_txt=['-> Test 12c : ' num2str(corrected_12c) ' automatic corrections suggested in the csv and json files'];
                    disp(msg_txt)
                    if opt_pause>0
                        disp('Press a key to continue ...')
                        pause
                    end
                    if opt_wrtlog>0
                        fprintf(fidLog,'%s\r\n',msg_txt);
                    end
                end
            end
        end
    else
        msg_txt=['-> Tests 12abc not run since index_rch_node has not been calculated (boost mode not activated)!'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    % ===========================TEST 13 ======================================
    % If we test for sets (opt_filter_validity=1), and if the up & dn Reaches have only 1 outside connected Reach, then it should be Type 1.
    % Check if Reaches are from upstream to downstream (not done here ...)
    ok_test13=0;
    type_test13(1:6)=zeros(1,6);
    if opt_filter_validity>0
        ref_index_rch=ref_rch(1);
        if n_rch_up_rch(ref_index_rch)==1
            id1=rch_id_up_rch(ref_index_rch,1);
            id1_txt=num2str(id1);
            id1_typ=id1_txt(end);
            if id1_typ==1
                ok_test13=ok_test13+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=13.0;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test13(id_rch_t(i))=type_test13(id_rch_t(i))+1;
            end
        end
        ref_index_rch=ref_rch(end);
        if n_rch_dn_rch(ref_index_rch)==1
            id1=rch_id_dn_rch(ref_index_rch,1);
            id1_txt=num2str(id1);
            id1_typ=id1_txt(end);
            if id1_typ==1
                ok_test13=ok_test13+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=13.0;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test13(id_rch_t(i))=type_test13(id_rch_t(i))+1;
            end
        end
        %         for ii=1:nb_ref
        %             ref_index_rch=ref_rch(ii);
        %         end
    end
    if ok_test13==0
        msg_txt=['+> Test 13 passed (Up & Dn Reaches of the set are not type 1)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 13 failed (Up or Dn Reaches of the set are type 1) ' num2str(ok_test13) ' times out of ' num2str(nb_nod_verified) ' Nodes (' num2str((ok_test13/nb_nod_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test13(i)>0
                msg_txt=['-> Test 13 per type ' num2str(i) ' = ' num2str((type_test13(i)/ok_test13)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % ===========================TEST 14 ======================================
    % Name of Rivers Reaches should not be NODATA ... : Test14a -> Correction available
    % Name of Rivers Reaches should be the same as upstream and downstream ones, if they are identical : Test14b -> Correction available
    % If a Reach River name is NODATA, but some upstream and downstream have a same name, or one of the 2 does not exist, then we
    % set it to this value (if opt_sword_corr_auto>0). Same test even if it is not a NODATA name, but we count it in another counter
    % (ok_test14b instead of ok_test14a)
    % We could also test for Nodes (not yet but correct_sword_reach_attribute does propagate the Name correction to Nodes)
    % We could loop over this test to accelerate the corrections
    ok_test14a=0; % Reach River name is NODATA
    ok_test14b=0; % Reach River name is different from its upstream and downstream ones (in case they are the same)
    type_test14a(1:6)=zeros(1,6);
    type_test14b(1:6)=zeros(1,6);
    corrected_14a=0;
    corrected_14b=0;
    for i=1:nb_rch
        list_name_up=[];
        list_name_dn=[];
        same_up_dn=[];
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        % We check the names of the upstream Reaches (if any)
        for j=1:n_rch_up_rch(i)
            id1=rch_id_up_rch(i,j); % jth Reach id connected upstream of Reach index i
            index1=find(id_rch==id1,1,'first'); % Its index
            if isempty(index1)
                continue
            end
            if ~isequal(river_name_rch(index1),'NODATA')
                list_name_up=[list_name_up river_name_rch(index1)];
            end
        end
        % We check the names of the downstream Reaches (if any)
        for j=1:n_rch_dn_rch(i)
            id1=rch_id_dn_rch(i,j); % jth Reach id connected downstream of Reach index i
            index1=find(id_rch==id1,1,'first'); % Its index
            if isempty(index1)
                continue
            end
            if ~isequal(river_name_rch(index1),'NODATA')
                list_name_dn=[list_name_dn river_name_rch(index1)];
            end
        end
        % Name of upstream and downstream Reaches when they are the same (and no NODATA), if any
        if ~isempty(list_name_up) && ~isempty(list_name_dn)
            same_up_dn=intersect(list_name_up,list_name_dn);
        elseif ~isempty(list_name_up)
            same_up_dn=list_name_up;
        elseif ~isempty(list_name_dn)
            same_up_dn=list_name_dn;
        else
            same_up_dn=[];
        end
        % We check the Name
        if opt_sword_corr_auto>0
            new_attribute_value=[];
        end
        if isequal(river_name_rch(i),'NODATA')
            msg_txt=['Name of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is NODATA'];
            if opt_disp_details>0
                disp(msg_txt)
            end
            if opt_wrtlog>1 && opt_wrtlog_details>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
            ok_test14a=ok_test14a+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=14.1;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=0;
            type_test14a(id_rch_t(i))=type_test14a(id_rch_t(i))+1;
            if opt_sword_corr_auto>0 && i_run>=i_run_min
                if ~isempty(same_up_dn)
                    disp(['We will set it as its neighbors ' char(same_up_dn(1)) ', since identical!'])
                    if length(same_up_dn)>1
                        for k=2:length(same_up_dn)
                            disp(['Other possible options would have been ' char(same_up_dn(k)) ', since identical (Option ' num2str(k) ')!' ])
                        end
                    end
                    % Probably Reach i has same Name as up & dn when identic
                    new_attribute_value=same_up_dn(1); % We take the first one. We may have more, but tricky then which to choose
                    corrected_14a=corrected_14a+1;
                end
            end
        else
            if ~isempty(same_up_dn) && isempty(intersect(river_name_rch(i),same_up_dn))
                msg_txt=['Name of Reach ' num2str(id_rch(i)) ' (' num2str(i) ') is ' char(river_name_rch(i)) ' so not the same as its neighbors ' char(same_up_dn(1)) ', though identical'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                ok_test14b=ok_test14b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=14.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=0;
                type_test14b(id_rch_t(i))=type_test14b(id_rch_t(i))+1;
                if opt_sword_corr_auto>0 && i_run>=i_run_min
                    if ~isempty(same_up_dn)
                        disp(['We will set it as its neighbors ' char(same_up_dn(1)) ', since identical!'])
                        if length(same_up_dn)>1
                            for k=2:length(same_up_dn)
                                disp(['Other possible options would have been ' char(same_up_dn(k)) ', since identical (Option ' num2str(k) ')!' ])
                            end
                        end
                        new_attribute_value=same_up_dn(1); % We take the first one. We may have more, but tricky then which to choose
                        corrected_14b=corrected_14b+1;
                    end
                end
            end
        end
        if opt_sword_corr_auto>0 && i_run>=i_run_min
            if ~isempty(new_attribute_value)
                patch_comment=['Old name=' char(river_name_rch(i))];
                disp(['Automatic correction: correct_sword_reach_attribute(' num2str(id_rch(i)) ',5,' char(new_attribute_value) ') : ' patch_comment])
                correct_sword_reach_attribute(id_rch(i),5,new_attribute_value,patch_comment);
            end
        end
    end
    if ok_test14a==0
        msg_txt=['+> Test 14a passed (all Reaches have a valid name, ie not NODATA)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 14a failed (some Reaches have NODATA name) ' num2str(ok_test14a) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test14a/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test14a(i)>0
                msg_txt=['-> Test 14a per type ' num2str(i) ' = ' num2str((type_test14a(i)/ok_test14a)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
        if opt_sword_corr_auto>0
            if corrected_14a>0
                msg_txt=['-> Test 14a : ' num2str(corrected_14a) ' automatic corrections suggested in the csv and json files'];
                disp(msg_txt)
                if opt_pause>0
                    disp('Press a key to continue ...')
                    pause
                end
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    if ok_test14b==0
        msg_txt=['+> Test 14b passed (all Reaches have a valid name, ie same as up & dn one when the same)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 14b failed (some Reaches have a different name than its up & dn one when the same) ' num2str(ok_test14b) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test14b/nb_rch_verified)*100,'%.3f') '%)'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test14b(i)>0
                msg_txt=['-> Test 14b per type ' num2str(i) ' = ' num2str((type_test14b(i)/ok_test14b)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
        if opt_sword_corr_auto>0
            if corrected_14b>0
                msg_txt=['-> Test 14b : ' num2str(corrected_14b) ' automatic corrections suggested in the csv and json files'];
                disp(msg_txt)
                if opt_pause>0
                    disp('Press a key to continue ...')
                    pause
                end
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end

    % ===========================TEST 15 ======================================
    % Check the River Reaches that are never seen ... and/or type 2-3-4-5-6
    ok_test15a=0; % Number of River Reaches never seen by SWOT
    ok_test15b=0; % Number of River Reaches Type 2
    ok_test15c=0; % Number of River Reaches Type 3
    ok_test15d=0; % Number of River Reaches Type 4
    ok_test15e=0; % Number of River Reaches Type 5
    ok_test15f=0; % Number of River Reaches Type 6
    ok_test15g=0; % Number of River Reaches Type 1
    km_test15a=0; % Number of km of River Reaches never seen by SWOT
    km_test15b=0; % Number of km of River Reaches Type 2
    km_test15c=0; % Number of km of River Reaches Type 3
    km_test15d=0; % Number of km of River Reaches Type 4
    km_test15e=0; % Number of km of River Reaches Type 5
    km_test15f=0; % Number of km of River Reaches Type 6
    km_test15g=0; % Number of km of River Reaches Type 1
    type_test15a(1:6)=zeros(1,6);
    % Compteurs pour les good Nodes
    ok_test15b_n=0; % Number of River Nodes Type 2
    ok_test15c_n=0; % Number of River Nodes Type 3
    ok_test15d_n=0; % Number of River Nodes Type 4
    ok_test15e_n=0; % Number of River Nodes Type 5
    ok_test15f_n=0; % Number of River Nodes Type 6
    ok_test15g_n=0; % Number of River Nodes Type 1
    km_test15b_n=0; % Number of km of River Nodes Type 2
    km_test15c_n=0; % Number of km of River Nodes Type 3
    km_test15d_n=0; % Number of km of River Nodes Type 4
    km_test15e_n=0; % Number of km of River Nodes Type 5
    km_test15f_n=0; % Number of km of River Nodes Type 6
    km_test15g_n=0; % Number of km of River Nodes Type 1
    % Compteurs pour les Reaches with wrong length wrt sum of its Node length
    ok_test15b_b=0; % Number of River Nodes Type 2
    ok_test15c_b=0; % Number of River Nodes Type 3
    ok_test15d_b=0; % Number of River Nodes Type 4
    ok_test15e_b=0; % Number of River Nodes Type 5
    ok_test15f_b=0; % Number of River Nodes Type 6
    ok_test15g_b=0; % Number of River Nodes Type 1
    km_test15b_b=0; % Number of km of River Nodes Type 2
    km_test15c_b=0; % Number of km of River Nodes Type 3
    km_test15d_b=0; % Number of km of River Nodes Type 4
    km_test15e_b=0; % Number of km of River Nodes Type 5
    km_test15f_b=0; % Number of km of River Nodes Type 6
    km_test15g_b=0; % Number of km of River Nodes Type 1
    tot_km_test15b_b=0; % Missing km for Type 2
    tot_km_test15c_b=0; % Missing km for Type 3
    tot_km_test15d_b=0; % Missing km for Type 4
    tot_km_test15e_b=0; % Missing km for Type 5
    tot_km_test15f_b=0; % Missing km for Type 6
    tot_km_test15g_b=0; % Missing km for Type 1
    error_max_km=0.2; % Threshold to consider that reach_length and sum of its Node is abnormal (in km)
    corrected_15b1=0; % Change the Node lengths from the geolocation (i_run=1)
    corrected_15b2=0; % Change the Reach lengths from the sum of Node length (i_run>1)
    for i=1:nb_rch
        if opt_filter_validity>0 && ~ismember(i,ref_rch)
            continue
        end
        if ~id_rch_filter(i)
            continue
        end
        % We check the number of times the Reach is seen by SWOT. If never we increment ok_test15
        if swot_obs(i)==0
            ok_test15a=ok_test15a+1;
            nb_case_test=nb_case_test+1;
            list_case_test(nb_case_test,1)=i_run;
            list_case_test(nb_case_test,2)=15.1;
            list_case_test(nb_case_test,3)=i;
            list_case_test(nb_case_test,4)=0;
            km_test15a=km_test15a+length_rch(i)/1000;
            type_test15a(id_rch_t(i))=type_test15a(id_rch_t(i))+1;
        end
        % For Reaches per Type. We compute the sum of the Reach lengths (in km)
        if id_rch_t(i)==2
            ok_test15b=ok_test15b+1;
            km_test15b=km_test15b+length_rch(i)/1000;
            km_test15b_b=0;
        elseif id_rch_t(i)==3
            ok_test15c=ok_test15c+1;
            km_test15c=km_test15c+length_rch(i)/1000;
            km_test15c_b=0;
        elseif id_rch_t(i)==4
            ok_test15d=ok_test15d+1;
            km_test15d=km_test15d+length_rch(i)/1000;
            km_test15d_b=0;
        elseif id_rch_t(i)==5
            ok_test15e=ok_test15e+1;
            km_test15e=km_test15e+length_rch(i)/1000;
            km_test15e_b=0;
        elseif id_rch_t(i)==6
            ok_test15f=ok_test15f+1;
            km_test15f=km_test15f+length_rch(i)/1000;
            km_test15f_b=0;
        elseif id_rch_t(i)==1
            ok_test15g=ok_test15g+1;
            km_test15g=km_test15g+length_rch(i)/1000;
            km_test15g_b=0;
        end
        % For Nodes per Type. We compute the sum of the Node lengths per Reach i (km_test15*_b) and in total (km_test15*_n) (in km)
        for j=index_rch_node(i,1):index_rch_node(i,2)
            if id_nod_rch(j)~=id_rch(i)
                % The Node j is not in the Reach i
                continue
            end
            if id_nod_t(j)==2
                ok_test15b_n=ok_test15b_n+1;
                km_test15b_n=km_test15b_n+length_nod(j)/1000;
                km_test15b_b=km_test15b_b+length_nod(j)/1000;
            elseif id_nod_t(j)==3
                ok_test15c_n=ok_test15c_n+1;
                km_test15c_n=km_test15c_n+length_nod(j)/1000;
                km_test15c_b=km_test15c_b+length_nod(j)/1000;
            elseif id_nod_t(j)==4
                ok_test15d_n=ok_test15d_n+1;
                km_test15d_n=km_test15d_n+length_nod(j)/1000;
                km_test15d_b=km_test15d_b+length_nod(j)/1000;
            elseif id_nod_t(j)==5
                ok_test15e_n=ok_test15e_n+1;
                km_test15e_n=km_test15e_n+length_nod(j)/1000;
                km_test15e_b=km_test15e_b+length_nod(j)/1000;
            elseif id_nod_t(j)==6
                ok_test15f_n=ok_test15f_n+1;
                km_test15f_n=km_test15f_n+length_nod(j)/1000;
                km_test15f_b=km_test15f_b+length_nod(j)/1000;
            elseif id_nod_t(j)==1
                ok_test15g_n=ok_test15g_n+1;
                km_test15g_n=km_test15g_n+length_nod(j)/1000;
                km_test15g_b=km_test15g_b+length_nod(j)/1000;
            end
        end
        pb_reach_nod_length=0;
        if id_rch_t(i)==2
            if abs(km_test15b_b-length_rch(i)/1000)>error_max_km
                new_length_rch=km_test15b_b*1000;
                pb_reach_nod_length=1;
                ok_test15b_b=ok_test15b_b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=15.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=km_test15b_b-length_rch(i)/1000;
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has a length ' num2str(length_rch(i)/1000,'%.3f') ' km different from the sum of its Nodes ' num2str(km_test15b_b,'%.3f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                tot_km_test15b_b=tot_km_test15b_b+abs(km_test15b_b-length_rch(i)/1000);
            end
        elseif id_rch_t(i)==3
            if abs(km_test15c_b-length_rch(i)/1000)>error_max_km
                new_length_rch=km_test15c_b*1000;
                pb_reach_nod_length=1;
                ok_test15c_b=ok_test15c_b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=15.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=km_test15c_b-length_rch(i)/1000;
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has a length ' num2str(length_rch(i)/1000,'%.3f') ' km different from the sum of its Nodes ' num2str(km_test15c_b,'%.3f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                tot_km_test15c_b=tot_km_test15c_b+abs(km_test15c_b-length_rch(i)/1000);
            end
        elseif id_rch_t(i)==4
            if abs(km_test15d_b-length_rch(i)/1000)>error_max_km
                new_length_rch=km_test15d_b*1000;
                pb_reach_nod_length=1;
                ok_test15d_b=ok_test15d_b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=15.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=km_test15d_b-length_rch(i)/1000;
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has a length ' num2str(length_rch(i)/1000,'%.3f') ' km different from the sum of its Nodes ' num2str(km_test15d_b,'%.3f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                tot_km_test15d_b=tot_km_test15d_b+abs(km_test15d_b-length_rch(i)/1000);
            end
        elseif id_rch_t(i)==5
            if abs(km_test15e_b-length_rch(i)/1000)>error_max_km
                new_length_rch=km_test15e_b*1000;
                pb_reach_nod_length=1;
                ok_test15e_b=ok_test15e_b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=15.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=km_test15e_b-length_rch(i)/1000;
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has a length ' num2str(length_rch(i)/1000,'%.3f') ' km different from the sum of its Nodes ' num2str(km_test15e_b,'%.3f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                tot_km_test15e_b=tot_km_test15e_b+abs(km_test15e_b-length_rch(i)/1000);
            end
        elseif id_rch_t(i)==6
            if abs(km_test15f_b-length_rch(i)/1000)>error_max_km
                new_length_rch=km_test15f_b*1000;
                pb_reach_nod_length=1;
                ok_test15f_b=ok_test15f_b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=15.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=km_test15f_b-length_rch(i)/1000;
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has a length ' num2str(length_rch(i)/1000,'%.3f') ' km different from the sum of its Nodes ' num2str(km_test15f_b,'%.3f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                tot_km_test15f_b=tot_km_test15f_b+abs(km_test15f_b-length_rch(i)/1000);
            end
        elseif id_rch_t(i)==1
            if abs(km_test15g_b-length_rch(i)/1000)>error_max_km
                new_length_rch=km_test15g_b*1000;
                pb_reach_nod_length=1;
                ok_test15g_b=ok_test15g_b+1;
                nb_case_test=nb_case_test+1;
                list_case_test(nb_case_test,1)=i_run;
                list_case_test(nb_case_test,2)=15.2;
                list_case_test(nb_case_test,3)=i;
                list_case_test(nb_case_test,4)=km_test15g_b-length_rch(i)/1000;
                msg_txt=['Reach ' num2str(id_rch(i)) ' (' num2str(i) ') has a length ' num2str(length_rch(i)/1000,'%.3f') ' km different from the sum of its Nodes ' num2str(km_test15g_b,'%.3f') ' km'];
                if opt_disp_details>0
                    disp(msg_txt)
                end
                if opt_wrtlog>1 && opt_wrtlog_details>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
                tot_km_test15g_b=tot_km_test15g_b+abs(km_test15g_b-length_rch(i)/1000);
            end
        end
        if pb_reach_nod_length>0
            % Traitement pour Type 1 à 6
            if i_run==i_run_min % Can change the Node length (with the one calculated from the geolocation) for the first run allowing correction i_run_min
                % There are different ways to compute the Node length
                list_nod=find(id_nod_rch==id_rch(i)); % All the Nodes indexes of the Reach i
                % We sort them increasing IDs, so downstream to upstream
                % But from Sword v17c this is not true any more. So we have to use new variables
                if isequal(cas_sword_version,'v17c')
                    if id_rch_nod(i,1)<id_rch_nod(i,2)
                        [~,order_nod]=sort(id_nod(list_nod),'ascend');
                    else
                        [~,order_nod]=sort(id_nod(list_nod),'descend');
                    end
                else
                    [~,order_nod]=sort(id_nod(list_nod),'ascend');
                end
                list_nod=list_nod(order_nod);
                for ij=1:length(list_nod)
                    j1=list_nod(ij); % A given Node index of this Reach i
                    % % Method 1 from the geolocation of its ctl points
                    % This is the good method, but difficult since the centerline points are sometimes strange
                    % This could be possible but need more work. They are bugs in the ctl point mapping to Nodes, including
                    % in v17b, cf email with Elizabeth on May, 20th 2025
                    % list_ctl=find(any(id_ctl_nod==id_nod(j1),2)); % All the centerline points indexes of the Node j1
                    % if length(list_ctl)>abs(id_nod_ctl(j1,2)-id_nod_ctl(j1,1))+1
                    %     disp(['length(list_ctl)=' num2str(length(list_ctl)) ', from id_nod_ctl=' num2str(abs(id_nod_ctl(j1,2)-id_nod_ctl(j1,1))+1)])
                    %     disp('Press a key to continue ...')
                    %     pause
                    % end
                    % for k=list_ctl
                    %     if id_ctl(k)<id_nod_ctl(j1,1) || id_ctl(k)>id_nod_ctl(j1,2)
                    %     disp(['id_ctl(k)=' num2str(id_ctl(k)) ', id_nod_ctl(j1,1)=' num2str(id_nod_ctl(j1,1)) ', id_nod_ctl(j1,2)=' num2str(id_nod_ctl(j1,2))])
                    %         disp('Press a key to continue ...')
                    %         pause
                    %     end
                    % end
                    % for ik=1:length(list_ctl)
                    %     k1=list_ctl(ik);
                    %     if ik==1
                    %         new_length_nod1=30; % On met 30m pour compenser les 2 bouts
                    %     else
                    %         k2=list_ctl(ik-1);
                    %         new_length_nod1=new_length_nod1+lldistkm([y_ctl(k1) x_ctl(k1)],[y_ctl(k2) x_ctl(k2)])*1000;
                    %     end
                    % end
                    % Method 2 from the geolocation of its next upstream and downstream Nodes
                    if ij==1 % For the first Node
                        if length(list_nod)>1
                            % There are at least 2 Nodes, we will get this distance
                            % Not the best solution, but difficult to do better ...
                            j2=list_nod(ij+1); % The next Node (upstream)
                            new_length_nod2=lldistkm([y_nod(j1) x_nod(j1)],[y_nod(j2) x_nod(j2)])*1000;
                        else
                            new_length_nod2=length_nod(j1); % Un seul noeud on ne peut rien calculer facilement, on garde la valeur initiale
                        end
                    else
                        j2=list_nod(ij-1); % The previous Node (downstream)
                        % We compute the distance between the 2 Nodes from their geolocation
                        new_length_nod2=lldistkm([y_nod(j1) x_nod(j1)],[y_nod(j2) x_nod(j2)])*1000;
                    end
                    % Comparaison des 2 méthodes
                    % if abs(new_length_nod2-new_length_nod1)>50
                    %     disp(['new_length_nod1=' num2str(new_length_nod1) ', new_length_nod2=' num2str(new_length_nod2)])
                    %     disp('Press a key to continue ...')
                    %     pause
                    % else
                    % new_length_nod=(new_length_nod1+new_length_nod2)/2;
                    new_length_nod=new_length_nod2;
                    % end
                    % Test de longueur
                    if abs(length_nod(j1)-new_length_nod)>gap_length_min_node && new_length_nod<length_max_node % Seuils (en m)
                        new_attribute_value=new_length_nod;
                        nb_case_test=nb_case_test+1;
                        list_case_test(nb_case_test,1)=i_run;
                        list_case_test(nb_case_test,2)=15.3;
                        list_case_test(nb_case_test,3)=j1;
                        list_case_test(nb_case_test,4)=length_nod(j1);
                    else
                        new_attribute_value=[];
                    end
                    if opt_sword_corr_auto>0 && ~isempty(new_attribute_value)
                        patch_comment=['Old length=' num2str(length_nod(j1),'%.3f') ' m'];
                        disp(['Automatic correction: correct_sword_node_attribute(' num2str(id_nod(j1)) ',6,' num2str(new_attribute_value) ') : ' patch_comment])
                        correct_sword_node_attribute(id_nod(j1),6,new_attribute_value,patch_comment);
                        corrected_15b1=corrected_15b1+1;
                    end
                end
            elseif i_run>i_run_min % Can change the Reach length (with the one calculated from the sum of the Node lengths) for the next runs > i_run_min
                if abs(length_rch(i)-new_length_rch)>gap_length_min_reach && new_length_rch<length_max_reach % Seuils (en m)
                    new_attribute_value=new_length_rch;
                    nb_case_test=nb_case_test+1;
                    list_case_test(nb_case_test,1)=i_run;
                    list_case_test(nb_case_test,2)=15.4;
                    list_case_test(nb_case_test,3)=i;
                    list_case_test(nb_case_test,4)=length_rch(i);
                else
                    if opt_sword_corr_auto>0 && i_run==i_run_min+2
                        % We limit to i_run==i_run_min+2 not to have the same message at all next iterations (since the pb is
                        % not corrected
                        % There is a problem but we do not correct it due to the thresholds gap_length_min_reach or length_max_reach
                        patch_comment=['Old length=' num2str(length_rch(i),'%.3f') ' m, New tentative length=' num2str(new_length_rch,'%.3f') ' m'];
                        disp(['Automatic correction: correct_sword_other(' num2str(id_rch(i)) ',' patch_comment])
                        correct_sword_other(id_rch(i),patch_comment);
                    end
                    new_attribute_value=[];
                end
                if opt_sword_corr_auto>0 && ~isempty(new_attribute_value)
                    patch_comment=['Old length=' num2str(length_rch(i),'%.3f') ' m'];
                    disp(['Automatic correction: correct_sword_reach_attribute(' num2str(id_rch(i)) ',6,' num2str(new_attribute_value) ') : ' patch_comment])
                    correct_sword_reach_attribute(id_rch(i),6,new_attribute_value,patch_comment);
                    corrected_15b2=corrected_15b2+1;
                end
            end
        end
    end
    if ok_test15a==0
        msg_txt=['+> Test 15a passed (all Reaches are seen at least once by SWOT)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15a failed (some Reaches are never seen by SWOT) ' num2str(ok_test15a) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15a/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15a,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
        for i=1:6
            if type_test15a(i)>0
                msg_txt=['-> Test 15a per type ' num2str(i) ' = ' num2str((type_test15a(i)/ok_test15a)*100,'%.3f') '%'];
                disp(msg_txt)
                if opt_wrtlog>0
                    fprintf(fidLog,'%s\r\n',msg_txt);
                end
            end
        end
    end
    % For Reaches of Types 1 to 6
    if ok_test15b==0
        msg_txt=['+> Test 15b passed (no type 2 Reach)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15b failed (some Reaches are type 2) ' num2str(ok_test15b) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15b/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15c==0
        msg_txt=['+> Test 15c passed (no type 3 Reach)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15c failed (some Reaches are type 3) ' num2str(ok_test15c) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15c/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15c,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15d==0
        msg_txt=['+> Test 15d passed (no type 4 Reach)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15d failed (some Reaches are type 4) ' num2str(ok_test15d) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15d/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15d,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15e==0
        msg_txt=['+> Test 15e passed (no type 5 Reach)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15e failed (some Reaches are type 5) ' num2str(ok_test15e) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15e/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15e,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15f==0
        msg_txt=['+> Test 15f passed (no type 6 Reach)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15f failed (some Reaches are type 6) ' num2str(ok_test15f) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15f/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15f,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15g==0
        msg_txt=['+> Test 15g passed (no type 1 Reach)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15g failed (some Reaches are type 1) ' num2str(ok_test15g) ' times out of ' num2str(nb_rch_verified) ' Reaches (' num2str((ok_test15g/nb_rch_verified)*100,'%.3f') '%) corresponding to ' num2str(km_test15g,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    % For Nodes of Types 1 to 6
    % The number of declared Nodes (nb_nod_verified) can be different from the ones found. So we compute this sum
    nb_nod_found=ok_test15b_n+ok_test15c_n+ok_test15d_n+ok_test15e_n+ok_test15f_n+ok_test15g_n;
    if ok_test15b_n==0
        msg_txt=['+> Test 15b_n passed (no type 2 Node)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15b_n failed (some Nodes are type 2) ' num2str(ok_test15b_n) ' times out of ' num2str(nb_nod_found) ' Nodes (' num2str((ok_test15b_n/nb_nod_found)*100,'%.3f') '%) corresponding to ' num2str(km_test15b_n,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15c_n==0
        msg_txt=['+> Test 15c_n passed (no type 3 Node)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15c_n failed (some Nodes are type 3) ' num2str(ok_test15c_n) ' times out of ' num2str(nb_nod_found) ' Nodes (' num2str((ok_test15c_n/nb_nod_found)*100,'%.3f') '%) corresponding to ' num2str(km_test15c_n,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15d_n==0
        msg_txt=['+> Test 15d_n passed (no type 4 Node)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15d_n failed (some Nodes are type 4) ' num2str(ok_test15d_n) ' times out of ' num2str(nb_nod_found) ' Nodes (' num2str((ok_test15d_n/nb_nod_found)*100,'%.3f') '%) corresponding to ' num2str(km_test15d_n,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15e_n==0
        msg_txt=['+> Test 15e_n passed (no type 5 Node)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15e_n failed (some Nodes are type 5) ' num2str(ok_test15e_n) ' times out of ' num2str(nb_nod_found) ' Nodes (' num2str((ok_test15e_n/nb_nod_found)*100,'%.3f') '%) corresponding to ' num2str(km_test15e_n,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15f_n==0
        msg_txt=['+> Test 15f_n passed (no type 6 Node)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15f_n failed (some Nodes are type 6) ' num2str(ok_test15f_n) ' times out of ' num2str(nb_nod_found) ' Nodes (' num2str((ok_test15f_n/nb_nod_found)*100,'%.3f') '%) corresponding to ' num2str(km_test15f_n,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15g_n==0
        msg_txt=['+> Test 15g_n passed (no type 1 Node)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15g_n failed (some Nodes are type 1) ' num2str(ok_test15g_n) ' times out of ' num2str(nb_nod_found) ' Nodes (' num2str((ok_test15g_n/nb_nod_found)*100,'%.3f') '%) corresponding to ' num2str(km_test15g_n,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    % Same for Reaches with wrong length wrt sum of its Node length
    nb_rch_bad_found=ok_test15b_b+ok_test15c_b+ok_test15d_b+ok_test15e_b+ok_test15f_b+ok_test15g_b;
    if ok_test15b_b==0
        msg_txt=['+> Test 15b_b passed (no type 2 Reach with wrong length wrt sum of its Node length)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15b_b failed (some Reach length pb wrt Node length are type 2) ' num2str(ok_test15b_b) ' times out of ' num2str(nb_rch_bad_found) ' Reaches (' num2str((ok_test15b_b/nb_rch_bad_found)*100,'%.3f') '%) corresponding to ' num2str(tot_km_test15b_b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15c_b==0
        msg_txt=['+> Test 15c_b passed (no type 3 Reach with wrong length wrt sum of its Node length)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15c_b failed (some Reach length pb wrt Node length are type 3) ' num2str(ok_test15c_b) ' times out of ' num2str(nb_rch_bad_found) ' Reaches (' num2str((ok_test15c_b/nb_rch_bad_found)*100,'%.3f') '%) corresponding to ' num2str(tot_km_test15c_b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15d_b==0
        msg_txt=['+> Test 15d_b passed (no type 4 Reach with wrong length wrt sum of its Node length)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15d_b failed (some Reach length pb wrt Node length are type 4) ' num2str(ok_test15d_b) ' times out of ' num2str(nb_rch_bad_found) ' Reaches (' num2str((ok_test15d_b/nb_rch_bad_found)*100,'%.3f') '%) corresponding to ' num2str(tot_km_test15d_b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15e_b==0
        msg_txt=['+> Test 15e_b passed (no type 5 Reach with wrong length wrt sum of its Node length)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15e_b failed (some Reach length pb wrt Node length are type 5) ' num2str(ok_test15e_b) ' times out of ' num2str(nb_rch_bad_found) ' Reaches (' num2str((ok_test15e_b/nb_rch_bad_found)*100,'%.3f') '%) corresponding to ' num2str(tot_km_test15e_b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15f_b==0
        msg_txt=['+> Test 15f_b passed (no type 6 Reach with wrong length wrt sum of its Node length)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15f_b failed (some Reach length pb wrt Node length are type 6) ' num2str(ok_test15f_b) ' times out of ' num2str(nb_rch_bad_found) ' Reaches (' num2str((ok_test15f_b/nb_rch_bad_found)*100,'%.3f') '%) corresponding to ' num2str(tot_km_test15f_b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if ok_test15g_b==0
        msg_txt=['+> Test 15g_b passed (no type 1 Reach with wrong length wrt sum of its Node length)'];
        disp(msg_txt)
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    else
        msg_txt=['-> Test 15g_b failed (some Reach length pb wrt Node length are type 1) ' num2str(ok_test15g_b) ' times out of ' num2str(nb_rch_bad_found) ' Reaches (' num2str((ok_test15g_b/nb_rch_bad_found)*100,'%.3f') '%) corresponding to ' num2str(tot_km_test15g_b,'%.1f') ' km'];
        disp(msg_txt)
        if opt_pause>0
            disp('Press a key to continue ...')
            pause
        end
        if opt_wrtlog>0
            fprintf(fidLog,'%s\r\n',msg_txt);
        end
    end
    if opt_sword_corr_auto>0
        if corrected_15b1>0
            msg_txt=['-> Test 15*_b : ' num2str(corrected_15b1) ' automatic corrections for Node length suggested in the csv and json files'];
            disp(msg_txt)
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        end
        if corrected_15b2>0
            msg_txt=['-> Test 15*_b : ' num2str(corrected_15b2) ' automatic corrections for Reach length suggested in the csv and json files'];
            disp(msg_txt)
            if opt_pause>0
                disp('Press a key to continue ...')
                pause
            end
            if opt_wrtlog>0
                fprintf(fidLog,'%s\r\n',msg_txt);
            end
        end
    end

    % ===========================TEST ?? ======================================
    % Nodes of different Reaches close enough (200m) should be in connected Reaches ?
    % Centerline points ID & Index should be ordered from Downstream to Upstream ?
    % Distance between 2 centerline points should be around 30 m
    % WSE should decrease from upstream to downstream

    if opt_sword_corr_auto==0 || (i_run>=i_run_min && ok_test1==0 && ok_test2==0 && ok_test3==0 && ok_test4==0 && corrected_11==0 ...
            && corrected_12c==0 && corrected_14a==0 && corrected_14b==0 && corrected_15b1==0 && corrected_15b2==0)
        % && ok_test5a==0 && ok_test5b==0 && ok_test6==0 && ok_test7a==0 && ok_test7b==0
        % && ok_test8a==0 && ok_test8b==0 && ok_test9a==0 && ok_test9b==0 && ok_test10a==0 && ok_test10b==0
        % && ok_test10c==0 && ok_test10d==0 && ok_test12a==0 && ok_test12b==0 && ok_test12c==0 && ok_test13==0
        if opt_sword_corr_auto==0
            disp(['The automatic correction mode (opt_sword_corr_auto) was not activated!'])
        end
        disp(['No correction was done during run ' num2str(i_run) '. We stop the validation and correction there!'])
        if run_stop==1
            break
        else
            disp(['We make a last run to get the correct_sword_other informations!'])
            ok_correct_sword_other=1;
            run_stop=1;
        end
    else
        if i_run<i_run_max
            disp(['Some corrections were done during run ' num2str(i_run) '. We continue the validation and correction with another run!'])
            struct_reach=[];
            list_report1=[];
            list_report2=[];
            list_report3=[];
            list_report4=[];
            list_report5=[];
            list_report6=[];
            list_report7=[];
            case_report4=[];
            case_report7=[];
            if i_run==i_run_max-1
                disp(['We make a last run to get the correct_sword_other informations!'])
                ok_correct_sword_other=1;
            end
        else
            disp(['Some corrections were done during run ' num2str(i_run) '. But we reached the maximum number of runs allowed, so we stop there!'])
        end
        if ok_test1>0
            disp(['Test1 corrections: ' num2str(ok_test1)])
        end
        if ok_test2>0
            disp(['Test2 corrections: ' num2str(ok_test2)])
        end
        if ok_test3>0
            disp(['Test3 corrections: ' num2str(ok_test3)])
        end
        if ok_test4>0
            disp(['Test4 corrections: ' num2str(ok_test4)])
        end
        if corrected_11>0
            disp(['Test11 corrections: ' num2str(corrected_11)])
        end
        if corrected_12c>0
            disp(['Test12c corrections: ' num2str(corrected_12c)])
        end
        if corrected_14a>0
            disp(['Test14a corrections: ' num2str(corrected_14a)])
        end
        if corrected_14b>0
            disp(['Test14b corrections: ' num2str(corrected_14b)])
        end
        if corrected_15b1>0
            disp(['Test15b corrections (Nodes): ' num2str(corrected_15b1)])
        end
        if corrected_15b2>0
            disp(['Test15b corrections (Reaches): ' num2str(corrected_15b2)])
        end
    end
end
% =========================================================================
if opt_sword_corr_auto>0 && opt_create_json>0
    % Sauvegarde et fermeture du fichier json
    txt_json=jsonencode(struct_patch_json,'PrettyPrint',true);
    fprintf(fidJson,'%s\r\n',txt_json);
    fclose(fidJson);
    % Sauvegarde de la structure (pour gestion du json du mode boost, dans sword_compute)
    struct_patch_automatic_json=struct_patch_json;
end
% =========================================================================
if option_kml_tests>0 && exist('list_case_test','var') && ~isempty(list_case_test)
    % Sauvegarde des résultats des tests dans des fichiers kml
    lapstime=tic;
    disp('Kml Tests files printing ...')
    Liste_Tests=[1.0,1.1,1.2,1.3,2.0,2.1,2.2,2.3,3.1,3.2,3.3,3.4,3.5,4.1,4.2,4.3,4.4,4.5];
    Liste_Tests=[Liste_Tests,5.1,5.2,6.1,6.2,7.1,7.2,8.1,8.2,9.1,9.2,9.3,9.4,10.1,10.2,10.3,10.4];
    Liste_Tests=[Liste_Tests,11.1,11.2,11.3,11.4,12.1,12.2,12.3,13.0,14.1,14.2,15.1,15.2,15.3,15.4];
    % list_case_test(nb_case_test,1)=i_run;
    % list_case_test(nb_case_test,2)=5.1;
    % list_case_test(nb_case_test,3)=i;
    for type_test=Liste_Tests
        % Test pb (red)
        filekml=[dir_out '\Tests_' num2str(round(type_test*10)) '_' nom_riv '.kml'];
        data_tests=find(abs(list_case_test(:,2)-type_test)<0.05);
        if length(data_tests)>0
            if ismember(round(type_test*10),[93 94])
                kml_format='%.1f'; % Pour ces tests on a aussi une décimale d'indication du code d'erreur
            else
                kml_format='%.0f';
            end
            if ismember(round(type_test*10),[62 91 94 101 102 103 104 153]) % Nodes
                if option_kml_tests>1 % On écrit le nom de l'ID (Node)
                    names=string(num2str(id_nod(list_case_test(data_tests,3))));
                    if option_kml_tests>2 % On écrit en plus un numérique informatif de l'erreur détectée
                        names=names+" - "+num2str(list_case_test(data_tests,4),kml_format);
                    end
                else
                    names='.';
                end
                y_test=y_nod(abs(list_case_test(data_tests,3)));
                x_test=x_nod(abs(list_case_test(data_tests,3)));
            else % Reaches
                if option_kml_tests>1 % On écrit le nom de l'ID (Reach)
                    names=string(num2str(id_rch(list_case_test(data_tests,3))));
                    if option_kml_tests>2
                        names=names+" - "+num2str(list_case_test(data_tests,4),kml_format);
                    end
                else
                    names='.';
                end
                y_test=y_rch(list_case_test(data_tests,3));
                x_test=x_rch(list_case_test(data_tests,3));
            end
            kmlwrite(filekml,y_test,x_test,'Color','red','Name',names)
            disp(['Kml Test file for ' num2str(type_test) ' created with ' num2str(length(data_tests)) ' items!'])
        else
            disp(['No Test ' num2str(type_test) ' to write on the kml file!'])
            % disp('Press a key to continue ...')
            % pause
        end
    end
    time_duration=toc(lapstime);
    disp(['Kml Tests file printing finished: ' num2str(time_duration) ' s'])
end
% =========================================================================
time_duration=toc(lapstime);
disp(['Verification of some properties of the Sword database finished: ' num2str(time_duration) ' s'])

