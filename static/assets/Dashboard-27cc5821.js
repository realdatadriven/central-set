import{S as gt,i as wt,s as kt,z as Je,I as Ne,e as M,t as pe,a as U,c as oe,b as w,A as je,B as ze,d as ae,f as b,m as ce,l as W,n as Ee,g as N,h as ve,j as B,k as De,o as he,p as fe,D as Ve,q as tt,_ as pt,r as vt,u as Dt,F as nt,C as Te,v as Le,w as dt,x as bt,T as qt,H as St,M as Mt,J as Ft,K as Nt,L as Et,y,W as jt,a4 as zt,$ as Bt,N as Ot,a5 as At,P as Jt,a0 as Tt,a1 as st,U as qe,a8 as rt}from"./index-13d4ac78.js";import"./pt-607305e9.js";import Lt from"./DataViz-740d4f3c.js";function lt(s,n,t){const r=s.slice();return r[51]=n[t],r[52]=n,r[53]=t,r}function it(s,n,t){const r=s.slice();return r[54]=n[t],r}function ot(s){let n,t,r,l,f;return{c(){var h,k;n=M("div"),t=M("input"),w(t,"type","text"),w(t,"class","input input-bordered input-sm flex-grow"),w(n,"class","w-3/12 pl-4"),w(n,"title",r=je((k=(h=s[1])==null?void 0:h.dashboard_conf)==null?void 0:k.label,30)),ze(n,"display",s[3]?null:"none")},m(h,k){ae(h,n,k),b(n,t),Te(t,s[1].dashboard_conf.label),l||(f=[W(t,"input",s[24]),W(n,"dblclick",s[25])],l=!0)},p(h,k){var o,_;k[0]&130&&t.value!==h[1].dashboard_conf.label&&Te(t,h[1].dashboard_conf.label),k[0]&130&&r!==(r=je((_=(o=h[1])==null?void 0:o.dashboard_conf)==null?void 0:_.label,30))&&w(n,"title",r),k[0]&8&&ze(n,"display",h[3]?null:"none")},d(h){h&&he(n),l=!1,Ve(f)}}}function ct(s){var h,k;let n,t,r=Le(((k=(h=s[1])==null?void 0:h.dashboard_conf)==null?void 0:k.items)||[]),l=[];for(let o=0;o<r.length;o+=1)l[o]=_t(lt(s,r,o));const f=o=>B(l[o],1,1,()=>{l[o]=null});return{c(){for(let o=0;o<l.length;o+=1)l[o].c();n=dt()},m(o,_){for(let a=0;a<l.length;a+=1)l[a]&&l[a].m(o,_);ae(o,n,_),t=!0},p(o,_){var a,m;if(_[0]&30854){r=Le(((m=(a=o[1])==null?void 0:a.dashboard_conf)==null?void 0:m.items)||[]);let i;for(i=0;i<r.length;i+=1){const p=lt(o,r,i);l[i]?(l[i].p(p,_),N(l[i],1)):(l[i]=_t(p),l[i].c(),N(l[i],1),l[i].m(n.parentNode,n))}for(ve(),i=r.length;i<l.length;i+=1)f(i);De()}},i(o){if(!t){for(let _=0;_<r.length;_+=1)N(l[_]);t=!0}},o(o){l=l.filter(Boolean);for(let _=0;_<l.length;_+=1)B(l[_]);t=!1},d(o){o&&he(n),bt(l,o)}}}function ft(s){let n,t,r,l;const f=[It,Ht,Ct,Vt],h=[];function k(o,_){var a,m,i,p,q,j,L,g;return(a=o[51])!=null&&a.loading?0:((p=(i=o[7])==null?void 0:i[(m=o[51])==null?void 0:m._id])==null?void 0:p.sucess)===!1?1:((q=o[51])==null?void 0:q.component)==="Filter"?2:(g=(L=o[7])==null?void 0:L[(j=o[51])==null?void 0:j._id])!=null&&g.data?3:-1}return~(n=k(s))&&(t=h[n]=f[n](s)),{c(){t&&t.c(),r=dt()},m(o,_){~n&&h[n].m(o,_),ae(o,r,_),l=!0},p(o,_){let a=n;n=k(o),n===a?~n&&h[n].p(o,_):(t&&(ve(),B(h[a],1,1,()=>{h[a]=null}),De()),~n?(t=h[n],t?t.p(o,_):(t=h[n]=f[n](o),t.c()),N(t,1),t.m(r.parentNode,r)):t=null)},i(o){l||(N(t),l=!0)},o(o){B(t),l=!1},d(o){o&&he(r),~n&&h[n].d(o)}}}function Vt(s){var r,l,f,h,k;let n,t;return n=new Lt({props:{_id:(r=s[51])==null?void 0:r._id,component:(l=s[51])==null?void 0:l.component,options:y.cloneDeep(s[51]),data:((k=(h=s[7])==null?void 0:h[(f=s[51])==null?void 0:f._id])==null?void 0:k.data)||[]}}),{c(){oe(n.$$.fragment)},m(o,_){ce(n,o,_),t=!0},p(o,_){var m,i,p,q,j;const a={};_[0]&2&&(a._id=(m=o[51])==null?void 0:m._id),_[0]&2&&(a.component=(i=o[51])==null?void 0:i.component),_[0]&2&&(a.options=y.cloneDeep(o[51])),_[0]&130&&(a.data=((j=(q=o[7])==null?void 0:q[(p=o[51])==null?void 0:p._id])==null?void 0:j.data)||[]),n.$set(a)},i(o){t||(N(n.$$.fragment,o),t=!0)},o(o){B(n.$$.fragment,o),t=!1},d(o){fe(n,o)}}}function Ct(s){var k,o,_;let n,t,r,l=Le(((_=(o=s[7])==null?void 0:o[(k=s[51])==null?void 0:k._id])==null?void 0:_.data)||[]),f=[];for(let a=0;a<l.length;a+=1)f[a]=ut(it(s,l,a));function h(){s[33].call(n,s[51])}return{c(){var a,m;n=M("select");for(let i=0;i<f.length;i+=1)f[i].c();w(n,"class","select select-sm -select-bordered w-full m-0"),s[2][(m=(a=s[51])==null?void 0:a.options)==null?void 0:m.name]===void 0&&Tt(h)},m(a,m){var i,p;ae(a,n,m);for(let q=0;q<f.length;q+=1)f[q]&&f[q].m(n,null);st(n,s[2][(p=(i=s[51])==null?void 0:i.options)==null?void 0:p.name],!0),t||(r=[W(n,"change",h),W(n,"change",Kt)],t=!0)},p(a,m){var i,p,q,j,L;if(s=a,m[0]&130){l=Le(((q=(p=s[7])==null?void 0:p[(i=s[51])==null?void 0:i._id])==null?void 0:q.data)||[]);let g;for(g=0;g<l.length;g+=1){const u=it(s,l,g);f[g]?f[g].p(u,m):(f[g]=ut(u),f[g].c(),f[g].m(n,null))}for(;g<f.length;g+=1)f[g].d(1);f.length=l.length}m[0]&134&&st(n,s[2][(L=(j=s[51])==null?void 0:j.options)==null?void 0:L.name])},i:qe,o:qe,d(a){a&&he(n),bt(f,a),t=!1,Ve(r)}}}function Ht(s){var o,_,a;let n,t,r,l,f,h=((a=(_=s[7])==null?void 0:_[(o=s[51])==null?void 0:o._id])==null?void 0:a.msg)+"",k;return{c(){n=M("div"),t=rt("svg"),r=rt("path"),l=U(),f=M("span"),k=pe(h),w(r,"stroke-linecap","round"),w(r,"stroke-linejoin","round"),w(r,"stroke-width","2"),w(r,"d","M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"),w(t,"xmlns","http://www.w3.org/2000/svg"),w(t,"class","stroke-current shrink-0 h-6 w-6"),w(t,"fill","none"),w(t,"viewBox","0 0 24 24"),w(n,"class","alert alert-error")},m(m,i){ae(m,n,i),b(n,t),b(t,r),b(n,l),b(n,f),b(f,k)},p(m,i){var p,q,j;i[0]&130&&h!==(h=((j=(q=m[7])==null?void 0:q[(p=m[51])==null?void 0:p._id])==null?void 0:j.msg)+"")&&Ee(k,h)},i:qe,o:qe,d(m){m&&he(n)}}}function It(s){let n;return{c(){n=M("span"),w(n,"class","loading loading-spinner loading-xs")},m(t,r){ae(t,n,r)},p:qe,i:qe,o:qe,d(t){t&&he(n)}}}function ut(s){var h,k,o;let n,t=((o=s[54])==null?void 0:o[(k=(h=s[51])==null?void 0:h.options)==null?void 0:k.value_field])+"",r,l,f;return{c(){var _,a,m;n=M("option"),r=pe(t),l=U(),n.__value=f=(m=s[54])==null?void 0:m[(a=(_=s[51])==null?void 0:_.options)==null?void 0:a.label_field],Te(n,n.__value)},m(_,a){ae(_,n,a),b(n,r),b(n,l)},p(_,a){var m,i,p,q,j,L;a[0]&130&&t!==(t=((p=_[54])==null?void 0:p[(i=(m=_[51])==null?void 0:m.options)==null?void 0:i.value_field])+"")&&Ee(r,t),a[0]&130&&f!==(f=(L=_[54])==null?void 0:L[(j=(q=_[51])==null?void 0:q.options)==null?void 0:j.label_field])&&(n.__value=f,Te(n,n.__value))},d(_){_&&he(n)}}}function _t(s){var O,A,J;let n,t,r,l,f=(["MiniStat"].includes((O=s[51])==null?void 0:O.component)?"":Je((A=s[51])==null?void 0:A.label))+"",h,k,o,_,a,m,i,p,q,j,L,g,u,Y,C,Q,ee,X,Z=!["Blank"].includes((J=s[51])==null?void 0:J.component),ue,x,G,H,_e,$;i=new Ne({props:{type:"outline",icon:"pencil",path:"icons"}});function Se(){return s[30](s[53],s[51])}L=new Ne({props:{type:"outline",icon:"trash",path:"icons"}});function me(){return s[31](s[53],s[51])}C=new Ne({props:{type:"outline",icon:"document-duplicate",path:"icons"}});function te(){return s[32](s[53],s[51])}let D=Z&&ft(s);function be(...E){return s[34](s[53],s[51],...E)}function Me(...E){return s[35](s[53],s[51],...E)}function P(...E){return s[36](s[53],s[51],...E)}function T(...E){return s[37](s[53],s[51],...E)}return{c(){var E,I,ne,se,re,v,z,le;n=M("div"),t=M("div"),r=M("div"),l=M("span"),h=pe(f),o=U(),_=M("div"),a=M("button"),m=M("span"),oe(i.$$.fragment),p=U(),q=M("button"),j=M("span"),oe(L.$$.fragment),g=U(),u=M("button"),Y=M("span"),oe(C.$$.fragment),ee=U(),X=M("div"),D&&D.c(),x=U(),w(l,"class","flex-grow pl-2"),w(l,"title",k=je((E=s[51])==null?void 0:E.label)),w(m,"class","w-4 h-4"),w(a,"class","btn btn-square btn-ghost btn-xs"),w(j,"class","w-4 h-4"),w(q,"class","btn btn-square btn-ghost btn-xs"),w(Y,"class","w-4 h-4"),w(u,"class","btn btn-square btn-ghost btn-xs"),w(_,"class","w-auto flex flex-row justify-end flex-wrap overflow-x-hidden overflow-y-hidden"),w(r,"class",Q="card-title flex flex-row "+(["Filter"].includes((I=s[51])==null?void 0:I.component)?" !mb-0 ":"")+" "+(["MiniStat"].includes((ne=s[51])==null?void 0:ne.component)?" z-[1] !-mb-6 ":" text-xs border-b-2 border-b-slate-50 ")),w(X,"class",ue="card-body items-center text-center "+(["Filter","MiniStat","Blank_"].includes((se=s[51])==null?void 0:se.component)?"!p-0":"")),w(t,"class","card card-bordered shadow-xs rounded-md w-full card-compact"),ze(t,"minHeight",((re=s[51])==null?void 0:re.height)||"250px"),w(n,"class",G=(((v=s[51])==null?void 0:v.width)===12?`w-[${((((z=s[51])==null?void 0:z.width)||12)/12*100).toFixed(0)}%]`:`w-${((le=s[51])==null?void 0:le.width)||11}/12`)+" flex-none p-0.5 ---m-auto --border --border-red-500"),w(n,"draggable",!0)},m(E,I){ae(E,n,I),b(n,t),b(t,r),b(r,l),b(l,h),b(r,o),b(r,_),b(_,a),b(a,m),ce(i,m,null),b(_,p),b(_,q),b(q,j),ce(L,j,null),b(_,g),b(_,u),b(u,Y),ce(C,Y,null),b(t,ee),b(t,X),D&&D.m(X,null),b(n,x),H=!0,_e||($=[W(a,"click",Se),W(q,"click",me),W(u,"click",te),W(n,"dragstart",be),W(n,"dragover",Me),W(n,"drop",P),W(n,"dragend",T)],_e=!0)},p(E,I){var ne,se,re,v,z,le,ge,we,ke,e,c;s=E,(!H||I[0]&2)&&f!==(f=(["MiniStat"].includes((ne=s[51])==null?void 0:ne.component)?"":Je((se=s[51])==null?void 0:se.label))+"")&&Ee(h,f),(!H||I[0]&130&&k!==(k=je((re=s[51])==null?void 0:re.label)))&&w(l,"title",k),(!H||I[0]&130&&Q!==(Q="card-title flex flex-row "+(["Filter"].includes((v=s[51])==null?void 0:v.component)?" !mb-0 ":"")+" "+(["MiniStat"].includes((z=s[51])==null?void 0:z.component)?" z-[1] !-mb-6 ":" text-xs border-b-2 border-b-slate-50 ")))&&w(r,"class",Q),I[0]&2&&(Z=!["Blank"].includes((le=s[51])==null?void 0:le.component)),Z?D?(D.p(s,I),I[0]&2&&N(D,1)):(D=ft(s),D.c(),N(D,1),D.m(X,null)):D&&(ve(),B(D,1,1,()=>{D=null}),De()),(!H||I[0]&130&&ue!==(ue="card-body items-center text-center "+(["Filter","MiniStat","Blank_"].includes((ge=s[51])==null?void 0:ge.component)?"!p-0":"")))&&w(X,"class",ue),I[0]&2&&ze(t,"minHeight",((we=s[51])==null?void 0:we.height)||"250px"),(!H||I[0]&130&&G!==(G=(((ke=s[51])==null?void 0:ke.width)===12?`w-[${((((e=s[51])==null?void 0:e.width)||12)/12*100).toFixed(0)}%]`:`w-${((c=s[51])==null?void 0:c.width)||11}/12`)+" flex-none p-0.5 ---m-auto --border --border-red-500"))&&w(n,"class",G)},i(E){H||(N(i.$$.fragment,E),N(L.$$.fragment,E),N(C.$$.fragment,E),N(D),H=!0)},o(E){B(i.$$.fragment,E),B(L.$$.fragment,E),B(C.$$.fragment,E),B(D),H=!1},d(E){E&&he(n),fe(i),fe(L),fe(C),D&&D.d(),_e=!1,Ve($)}}}function at(s){let n,t;return n=new qt({props:{open:s[4].open,type:s[4].type,msg:s[4].msg,timer:2e4}}),n.$on("dismiss",s[38]),{c(){oe(n.$$.fragment)},m(r,l){ce(n,r,l),t=!0},p(r,l){const f={};l[0]&16&&(f.open=r[4].open),l[0]&16&&(f.type=r[4].type),l[0]&16&&(f.msg=r[4].msg),n.$set(f)},i(r){t||(N(n.$$.fragment,r),t=!0)},o(r){B(n.$$.fragment,r),t=!1},d(r){fe(n,r)}}}function ht(s){let n,t;const r=[s[5]];let l={};for(let f=0;f<r.length;f+=1)l=St(l,r[f]);return n=new Mt({props:l}),n.$on("dismiss",s[39]),{c(){oe(n.$$.fragment)},m(f,h){ce(n,f,h),t=!0},p(f,h){const k=h[0]&32?Ft(r,[Nt(f[5])]):{};n.$set(k)},i(f){t||(N(n.$$.fragment,f),t=!0)},o(f){B(n.$$.fragment,f),t=!1},d(f){fe(n,f)}}}function mt(s){let n,t;return n=new Et({props:{open:s[6]}}),{c(){oe(n.$$.fragment)},m(r,l){ce(n,r,l),t=!0},p(r,l){const f={};l[0]&64&&(f.open=r[6]),n.$set(f)},i(r){t||(N(n.$$.fragment,r),t=!0)},o(r){B(n.$$.fragment,r),t=!1},d(r){fe(n,r)}}}function Pt(s){var E,I,ne,se,re;let n,t,r,l,f=Je((I=(E=s[1])==null?void 0:E.dashboard_conf)==null?void 0:I.label,30)+"",h,k,o,_,a,m=s[8].t("crud.filter")+"",i,p,q=JSON.stringify(s[2])+"",j,L,g,u,Y,C,Q,ee,X,Z,ue,x,G,H,_e,$,Se,me,te,D,be,Me,P=((ne=s[1])==null?void 0:ne.dashboard_conf)&&ot(s);C=new Ne({props:{type:"outline",icon:"save",path:"icons"}}),Z=new Ne({props:{type:"outline",icon:"refresh",path:"icons"}}),H=new Ne({props:{type:"outline",icon:"trash",path:"icons"}});let T=((re=(se=s[1])==null?void 0:se.dashboard_conf)==null?void 0:re.items)&&ct(s),O=s[4].open&&at(s),A=s[5].open&&ht(s),J=s[6]&&mt(s);return{c(){var v,z;n=M("div"),t=M("div"),r=M("div"),l=M("span"),h=pe(f),o=U(),P&&P.c(),_=U(),a=M("div"),i=pe(m),p=pe("s: "),j=pe(q),L=U(),g=M("div"),u=M("button"),Y=M("span"),oe(C.$$.fragment),Q=U(),ee=M("button"),X=M("span"),oe(Z.$$.fragment),ue=U(),x=M("button"),G=M("span"),oe(H.$$.fragment),_e=U(),$=M("div"),T&&T.c(),Se=U(),O&&O.c(),me=U(),A&&A.c(),te=U(),J&&J.c(),w(l,"class","text-md font-bold text-primary"),w(r,"class","w-3/12 pl-4"),w(r,"title",k=je((z=(v=s[1])==null?void 0:v.dashboard_conf)==null?void 0:z.label,30)),ze(r,"display",s[3]?"none":null),w(a,"class","flex-grow flex flex-row overflow-x-auto overflow-y-hidden"),w(Y,"class","w-5 h-5"),w(u,"class","btn btn-square btn-ghost btn-sm"),w(X,"class","w-5 h-5"),w(ee,"class","btn btn-square btn-ghost btn-sm"),w(G,"class","w-5 h-5"),w(x,"class","btn btn-square btn-ghost btn-sm"),w(g,"class","w-auto flex flex-row justify-end flex-wrap overflow-x-hidden overflow-y-hidden"),w(t,"class","--navbar --navbar-xs w-full border-b-2 rounded-md border-slate-200 p-0 flex flex-row m-0.5 h-8"),w($,"class","w-full flex flex-row justify-start flex-wrap overflow-x-hidden overflow-y-auto"),w(n,"class","w-full p-2")},m(v,z){ae(v,n,z),b(n,t),b(t,r),b(r,l),b(l,h),b(t,o),P&&P.m(t,null),b(t,_),b(t,a),b(a,i),b(a,p),b(a,j),b(t,L),b(t,g),b(g,u),b(u,Y),ce(C,Y,null),b(g,Q),b(g,ee),b(ee,X),ce(Z,X,null),b(g,ue),b(g,x),b(x,G),ce(H,G,null),b(n,_e),b(n,$),T&&T.m($,null),b(n,Se),O&&O.m(n,null),b(n,me),A&&A.m(n,null),b(n,te),J&&J.m(n,null),D=!0,be||(Me=[W(r,"dblclick",s[23]),W(u,"click",s[26]),W(ee,"click",s[27]),W(x,"click",s[28]),W(t,"drop",s[29])],be=!0)},p(v,z){var le,ge,we,ke,e,c,S;(!D||z[0]&2)&&f!==(f=Je((ge=(le=v[1])==null?void 0:le.dashboard_conf)==null?void 0:ge.label,30)+"")&&Ee(h,f),(!D||z[0]&130&&k!==(k=je((ke=(we=v[1])==null?void 0:we.dashboard_conf)==null?void 0:ke.label,30)))&&w(r,"title",k),z[0]&8&&ze(r,"display",v[3]?"none":null),(e=v[1])!=null&&e.dashboard_conf?P?P.p(v,z):(P=ot(v),P.c(),P.m(t,_)):P&&(P.d(1),P=null),(!D||z[0]&256)&&m!==(m=v[8].t("crud.filter")+"")&&Ee(i,m),(!D||z[0]&4)&&q!==(q=JSON.stringify(v[2])+"")&&Ee(j,q),(S=(c=v[1])==null?void 0:c.dashboard_conf)!=null&&S.items?T?(T.p(v,z),z[0]&2&&N(T,1)):(T=ct(v),T.c(),N(T,1),T.m($,null)):T&&(ve(),B(T,1,1,()=>{T=null}),De()),v[4].open?O?(O.p(v,z),z[0]&16&&N(O,1)):(O=at(v),O.c(),N(O,1),O.m(n,me)):O&&(ve(),B(O,1,1,()=>{O=null}),De()),v[5].open?A?(A.p(v,z),z[0]&32&&N(A,1)):(A=ht(v),A.c(),N(A,1),A.m(n,te)):A&&(ve(),B(A,1,1,()=>{A=null}),De()),v[6]?J?(J.p(v,z),z[0]&64&&N(J,1)):(J=mt(v),J.c(),N(J,1),J.m(n,null)):J&&(ve(),B(J,1,1,()=>{J=null}),De())},i(v){D||(N(C.$$.fragment,v),N(Z.$$.fragment,v),N(H.$$.fragment,v),N(T),N(O),N(A),N(J),D=!0)},o(v){B(C.$$.fragment,v),B(Z.$$.fragment,v),B(H.$$.fragment,v),B(T),B(O),B(A),B(J),D=!1},d(v){v&&he(n),P&&P.d(),fe(C),fe(Z),fe(H),T&&T.d(),O&&O.d(),A&&A.d(),J&&J.d(),be=!1,Ve(Me)}}}const Kt=s=>{};function Rt(s,n,t){let r,l;tt(s,pt,e=>t(41,r=e)),tt(s,vt,e=>t(8,l=e));let{_id:f}=n,{table:h}=n,{data:k={}}=n,{aux_data:o={}}=n,{main_table:_=null}=n,{actions:a={}}=n,{action:m={}}=n,i={},p={},q=!1,j={label:"Not named dashboard",refresh_each_ms:1e4,filters:{},items:[{label:"item 1",width:4,height:"240px",component:"EChartsEngine",options:{type:""},raw_sql:!1,query:null,sql:null}]};const L=`key_${Math.floor(Math.random()*Date.now())}`;let g={open:!1,msg:null,type:null},u={open:!1,width:12,heigth:85,header:!0,title:"Dialog Title",header_actions:[],component:null},Y=!1;const C=async e=>{var V;const c=y.cloneDeep(e.data||i);(V=c==null?void 0:c.dashboard_conf)!=null&&V.items&&(c.dashboard_conf.filters={},c.dashboard_conf.items=c.dashboard_conf.items.map(K=>({...K,_id:null})),c.dashboard_conf=JSON.stringify(c.dashboard_conf)),console.log({_data:c}),t(6,Y=!0);const S={lang:r==null?void 0:r.lang,conf:r==null?void 0:r.conf,token:r==null?void 0:r.token,app:r==null?void 0:r.selected_app,table:e.table||h,data:c},F=await jt(S);if(F.success===!0){t(4,g.open=!0,g),t(4,g.type="success",g),t(4,g.msg=F.msg,g),F.inserted_primary_key&&t(1,i[r.tables[e.table].pk]=F.inserted_primary_key,i);try{m!=null&&m.action&&(m==null||m.action(i))}catch(K){console.log(K.message)}}else t(4,g.open=!0,g),t(4,g.type="error",g),t(4,g.msg=F.msg,g),F!=null&&F.errors&&F.errors.length>0&&(t(5,u.html_msg='<div class="alert alert-error shadow-lg m-1">'+F.errors.map(K=>K).join('</div><div class="alert alert-error shadow-lg m-1">')+"</div>",u),t(5,u.open=!0,u),t(5,u.header=!0,u),t(5,u.title=F.msg,u),t(5,u.actions=[{type:"btn",name:"OK",class:"text-primary--",label:l.t("crud.ok"),action:Q}],u),t(5,u.width=4,u),t(5,u.heigth=null,u));t(6,Y=!1)},Q=e=>{t(5,u.open=!1,u)},ee=async e=>{var S,F,V,K;const c={lang:r==null?void 0:r.lang,conf:r==null?void 0:r.conf,token:r==null?void 0:r.token,app:r==null?void 0:r.selected_app,table:e==null?void 0:e.table,limit:(e==null?void 0:e.limit)||-1,distinct:(e==null?void 0:e.distinct)||!1,database:(e==null?void 0:e.database)||null,fields:((F=(S=e==null?void 0:e.fields)==null?void 0:S.split(";"))==null?void 0:F.length)>1?(V=e==null?void 0:e.fields)==null?void 0:V.split(";"):((K=e==null?void 0:e.fields)==null?void 0:K.split(","))||null};return await Ot(c)},X=async e=>{var V,K,ie,de;let c={},S=null;try{c=JSON.parse((V=e==null?void 0:e.query)==null?void 0:V.manage_query_conf),S=(c==null?void 0:c.selected_tab)==="query_preview"?c==null?void 0:c.selected_tab_2_prev:c==null?void 0:c.selected_tab}catch{throw new Error(l.t("crud.no_query_setup"))}const F={class:"crud",method:"query",conf:r==null?void 0:r.conf,token:r==null?void 0:r.token,app:r==null?void 0:r.selected_app,data:{build_query:(ie=(K=c==null?void 0:c.tabs)==null?void 0:K[S])==null?void 0:ie.fields,filters:(e==null?void 0:e.filters)||p,query:e==null?void 0:e.sql,use_query_string:(e==null?void 0:e.raw_sql)||!1,database:(e==null?void 0:e.database)||((de=e==null?void 0:e.query)==null?void 0:de.database),limit:-1,offset:0}};return await At(F)},Z=e=>{console.log("_save_edited_item:",e),t(1,i.dashboard_conf.items[e==null?void 0:e.index]=y.cloneDeep(e==null?void 0:e.item),i),t(1,i={...i})},ue=async e=>{var S;t(6,Y=!0);const c=(await Jt(()=>import("./EditDashboardItem-c01d2f5b.js"),["assets/EditDashboardItem-c01d2f5b.js","assets/index-13d4ac78.js","assets/index-1fc5bb3b.css","assets/pt-607305e9.js","assets/ObjectEditor-dc630b79.js"])).default;if(t(6,Y=!1),e!=null&&e.show_in_popup)t(5,u.actions=null,u),t(5,u.component=c,u),t(5,u.msg=null,u),t(5,u.html_msg=null,u),t(5,u.header_actions=[{type:"icon",icon:"save",name:"SAVE",label:l.t("crud.save"),action:Z}],u),t(5,u.title=`${l.t("crud.edit")} ${l.t("crud.item")}`,u),t(5,u.data={item:y.cloneDeep(e==null?void 0:e._item),index:e==null?void 0:e.i},u),t(5,u.width=80,u),t(5,u.heigth=99,u),t(5,u.open=!0,u);else{const F=`${l.t("crud.edit")} ${l.t("crud.item")} - ${(S=e==null?void 0:e._item)==null?void 0:S.label} `;a.show_in_tabs({component:c,label:F,header_actions:[{type:"icon",icon:"save",name:"SAVE",label:l.t("crud.save"),action:Z}],show_top_close:!0,data:{label:F,table:e.table||h,item:y.cloneDeep(e==null?void 0:e._item),index:e==null?void 0:e.i,mode:"tab"}})}},x=e=>{switch(e==null?void 0:e.action){case"delete":i.dashboard_conf.items.splice(e==null?void 0:e.i,1),t(1,i={...i});break;case"duplicate":try{i.dashboard_conf.items.splice((e==null?void 0:e.i)+1,0,y.cloneDeep({...e==null?void 0:e._item,_id:null}))}catch{i.dashboard_conf.items.push(y.cloneDeep({...e==null?void 0:e._item,_id:null}))}t(1,i={...i});break;case"edit":ue(e);break}},G={},H=(e,c)=>{e.dataTransfer.setData("index",c==null?void 0:c.i),G.index=c==null?void 0:c.i},_e=(e,c)=>{G.index_dest=c==null?void 0:c.i},$=(e,c)=>{var S,F;try{e.preventDefault();const V=G.index,K=y.cloneDeep((F=(S=i==null?void 0:i.dashboard_conf)==null?void 0:S.items)==null?void 0:F[V]),ie=G.index_dest;i.dashboard_conf.items.splice(V,1),i.dashboard_conf.items.splice(ie,0,y.cloneDeep(K)),t(1,i={...i})}catch(V){t(4,g.open=!0,g),t(4,g.type="error",g),t(4,g.msg=V.message,g)}},Se=e=>{var c,S,F;if(e.data){const V={table:((c=e==null?void 0:e.data)==null?void 0:c.table)||h,permanently:(S=e==null?void 0:e.data)==null?void 0:S.permanently,data:(F=e==null?void 0:e.data)==null?void 0:F.data.map(K=>{var de,Fe,Be,Oe;const ie=(Be=(Fe=r==null?void 0:r.tables)==null?void 0:Fe[((de=e==null?void 0:e.data)==null?void 0:de.table)||h])==null?void 0:Be.pk;return{[ie]:K[ie],excluded:!0,_to_delete:!0,permanently:(Oe=e==null?void 0:e.data)==null?void 0:Oe.permanently}})};V.permanently,C(V)}Q()},me=e=>{var S,F;const c=l.t("crud.del_alert",{len:(S=e==null?void 0:e.data)==null?void 0:S.length,s:((F=e==null?void 0:e.data)==null?void 0:F.length)>1?"s":""});t(5,u.component=null,u),t(5,u.msg=null,u),t(5,u.html_msg=`<span class="m-2 text-lg">${c}</span>`,u),t(5,u.open=!0,u),t(5,u.header=!1,u),t(5,u.data=e,u),t(5,u.actions=[{type:"btn",icon:"ban",name:"NO",class:"btn-sm text-error",label:l.t("crud.no"),action:Q},{type:"btn",icon:"check",name:"YES",class:"btn-sm text-success",label:l.t("crud.yes"),action:Se}],u),t(5,u.width=4,u),t(5,u.heigth=null,u)},te=async e=>{if(e=e||k,!e)t(1,i={dashboard_conf:y.cloneDeep(j)});else if(!e.dashboard_conf)t(1,i={...e,dashboard_conf:y.cloneDeep(j)});else if(typeof e.dashboard_conf=="string"){t(2,p={}),t(7,D={});try{t(1,i={...e,dashboard_conf:JSON.parse(e.dashboard_conf)})}catch{t(1,i={...e,dashboard_conf:y.cloneDeep(j)})}}be(i)};let D={};const be=async e=>{var c,S,F,V,K,ie,de,Fe,Be,Oe,He,Ie,Pe,Ke,Re,Ue,We,Ye,Ge,Qe,Xe,Ze,xe,$e,ye;try{for(let R=0;R<(((S=(c=e==null?void 0:e.dashboard_conf)==null?void 0:c.items)==null?void 0:S.length)||0);R++){const d=(V=(F=e==null?void 0:e.dashboard_conf)==null?void 0:F.items)==null?void 0:V[R];if((d==null?void 0:d.component)!=="Blank"){(d==null?void 0:d.component)==="Filter"&&(p?Array.isArray(p)&&t(2,p={}):t(2,p={}),p[(K=d==null?void 0:d.options)==null?void 0:K.name]||t(2,p[(ie=d==null?void 0:d.options)==null?void 0:ie.name]=null,p)),e.dashboard_conf.items[R]._id||(e.dashboard_conf.items[R]._id=zt(`item-${d==null?void 0:d.label.replace(" ","_")}`),t(1,i={...i})),e.dashboard_conf.items[R].loading=!0;try{if(((de=d==null?void 0:d.options)==null?void 0:de.type)==="Read"?t(7,D[e.dashboard_conf.items[R]._id]=await ee(d==null?void 0:d.options),D):t(7,D[e.dashboard_conf.items[R]._id]=await X(d),D),(d==null?void 0:d.component)==="Filter"&&!p[(Fe=d==null?void 0:d.options)==null?void 0:Fe.name]){const Ae=(Oe=(Be=D[e.dashboard_conf.items[R]._id])==null?void 0:Be.data)==null?void 0:Oe.length;t(2,p[(He=d==null?void 0:d.options)==null?void 0:He.name]=(Re=(Pe=(Ie=D[e.dashboard_conf.items[R]._id])==null?void 0:Ie.data)==null?void 0:Pe[0])==null?void 0:Re[(Ke=d==null?void 0:d.options)==null?void 0:Ke.value_field],p),((Ue=d==null?void 0:d.options)==null?void 0:Ue.default)==="last"?t(2,p[(We=d==null?void 0:d.options)==null?void 0:We.name]=(Xe=(Ge=(Ye=D[e.dashboard_conf.items[R]._id])==null?void 0:Ye.data)==null?void 0:Ge[Ae-1])==null?void 0:Xe[(Qe=d==null?void 0:d.options)==null?void 0:Qe.value_field],p):((Ze=d==null?void 0:d.options)==null?void 0:Ze.default)==="---all"&&t(2,p[(xe=d==null?void 0:d.options)==null?void 0:xe.name]=(ye=($e=D[e.dashboard_conf.items[R]._id])==null?void 0:$e.data)==null?void 0:ye.map(Ce=>{var et;return Ce==null?void 0:Ce[(et=d==null?void 0:d.options)==null?void 0:et.value_field]}),p)}}catch(Ae){t(7,D[e.dashboard_conf.items[R]._id]={success:!1,msg:Ae==null?void 0:Ae.message},D)}delete e.dashboard_conf.items[R].loading}}}catch(R){console.log(R.message)}};Dt(async()=>{l.changeLanguage(r==null?void 0:r.lang),console.log({table:h,data:k,main_table:_,aux_data:o});try{nt.locale(r==null?void 0:r.lang)}catch{nt.locale("en-us")}te(k)});const Me=()=>{t(3,q=!q)};function P(){i.dashboard_conf.label=this.value,t(1,i),t(7,D)}const T=()=>{t(3,q=!q)},O=()=>{var e;return C({data:{...i,dashboard:(e=k==null?void 0:k.dashboard_conf)==null?void 0:e.label},table:"dashboard"})},A=()=>te(i),J=()=>{me({data:[i],table:"dashboard"})},E=e=>$(e),I=(e,c)=>x({i:e,_item:c,action:"edit"}),ne=(e,c)=>x({i:e,_item:c,action:"delete"}),se=(e,c)=>x({i:e,_item:c,action:"duplicate"});function re(e){var c;p[(c=e==null?void 0:e.options)==null?void 0:c.name]=Bt(this),t(2,p),t(7,D),t(1,i)}const v=(e,c,S)=>H(S,{i:e,_item:c}),z=(e,c,S)=>_e(S,{i:e,_item:c}),le=(e,c,S)=>$(S),ge=(e,c,S)=>$(S),we=e=>{t(4,g.open=!1,g)},ke=e=>Q(e.detail);return s.$$set=e=>{"_id"in e&&t(17,f=e._id),"table"in e&&t(18,h=e.table),"data"in e&&t(0,k=e.data),"aux_data"in e&&t(19,o=e.aux_data),"main_table"in e&&t(20,_=e.main_table),"actions"in e&&t(21,a=e.actions),"action"in e&&t(22,m=e.action)},s.$$.update=()=>{s.$$.dirty[0]&4456448&&((m==null?void 0:m.name)==="form_customization"||(m==null?void 0:m.name)==="save"&&(console.log(L,m),C({table:h}))),s.$$.dirty[0]&131072&&(console.log({_id:f}),te())},[k,i,p,q,g,u,Y,D,l,C,Q,x,H,_e,$,me,te,f,h,o,_,a,m,Me,P,T,O,A,J,E,I,ne,se,re,v,z,le,ge,we,ke]}class Gt extends gt{constructor(n){super(),wt(this,n,Rt,Pt,kt,{_id:17,table:18,data:0,aux_data:19,main_table:20,actions:21,action:22},null,[-1,-1])}}export{Gt as default};